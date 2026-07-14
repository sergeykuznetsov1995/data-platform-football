from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from dags.utils.transfermarkt_approval import (
    ApprovalJournal,
    ApprovalPacket,
    load_standing_policy,
)
from scrapers.transfermarkt.models import (
    FetchOutcome,
    FetchStatus,
    LeaseTrafficSnapshot,
    stable_payload_hash,
)
from scrapers.transfermarkt.registry import (
    AgeCategory,
    ClassificationEvidence,
    CompetitionRecord,
    CompetitionType,
    EditionRecord,
    EvidenceOrigin,
    Gender,
    RegistryPage,
    SeasonFormat,
    TeamType,
)


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "dags" / "scripts" / "run_transfermarkt_discovery.py"
NOW = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
SNAPSHOT_ID = "tm-discovery-" + "a" * 24


def _load():
    name = "run_transfermarkt_discovery_test_module"
    spec = importlib.util.spec_from_file_location(name, SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _evidence() -> tuple[ClassificationEvidence, ...]:
    return (
        ClassificationEvidence(
            source_field="catalog_section",
            source_value="National leagues | Men",
            source_url="https://www.transfermarkt.com/wettbewerbe/europa",
            origin=EvidenceOrigin.STRUCTURED,
            competition_type=CompetitionType.DOMESTIC_LEAGUE,
            gender=Gender.MEN,
            team_type=TeamType.CLUB,
            age_category=AgeCategory.SENIOR,
        ),
        ClassificationEvidence(
            source_field="edition_selector",
            source_value="2025/26",
            source_url="https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1",
            origin=EvidenceOrigin.STRUCTURED,
            season_format=SeasonFormat.SPLIT_YEAR,
        ),
    )


def _page(*, page_number: int = 1, page_count: int = 1) -> RegistryPage:
    eligible = CompetitionRecord(
        competition_id="GB1",
        slug="premier-league",
        name="Premier League",
        country="England",
        confederation="UEFA",
        competition_type=CompetitionType.DOMESTIC_LEAGUE,
        gender=Gender.MEN,
        team_type=TeamType.CLUB,
        age_category=AgeCategory.SENIOR,
        season_format=SeasonFormat.SPLIT_YEAR,
        active=True,
        source_url="https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1",
        discovered_at=NOW,
        evidence=_evidence(),
        registry_snapshot_id=SNAPSHOT_ID,
        source_body_hash="1" * 64,
    )
    unknown = CompetitionRecord(
        competition_id="UNK",
        slug="unknown-cup",
        name="Unknown Cup",
        country="World",
        confederation="Unknown",
        competition_type=CompetitionType.UNKNOWN,
        gender=Gender.UNKNOWN,
        team_type=TeamType.UNKNOWN,
        age_category=AgeCategory.UNKNOWN,
        season_format=SeasonFormat.SPLIT_YEAR,
        active=True,
        source_url="https://www.transfermarkt.com/unknown-cup/startseite/pokalwettbewerb/UNK",
        discovered_at=NOW,
        evidence=(
            ClassificationEvidence(
                source_field="edition_selector",
                source_value="2025/26",
                source_url="https://www.transfermarkt.com/unknown-cup/startseite/pokalwettbewerb/UNK",
                origin=EvidenceOrigin.STRUCTURED,
                season_format=SeasonFormat.SPLIT_YEAR,
            ),
        ),
        registry_snapshot_id=SNAPSHOT_ID,
        source_body_hash="2" * 64,
    )
    editions = tuple(
        EditionRecord(
            competition_id=competition_id,
            edition_id="2025",
            edition_label="2025/26",
            canonical_season="2526",
            season_format=SeasonFormat.SPLIT_YEAR,
            start_date="2025-08-01",
            end_date="2026-05-31",
            active=True,
            current=True,
            participant_count=20,
            participant_hash=body_hash,
            source_url=f"https://www.transfermarkt.com/{competition_id}/saison_id/2025",
            discovered_at=NOW,
            registry_snapshot_id=SNAPSHOT_ID,
            source_body_hash=body_hash,
        )
        for competition_id, body_hash in (("GB1", "3" * 64), ("UNK", "4" * 64))
    )
    return RegistryPage(
        snapshot_id=SNAPSHOT_ID,
        page_number=page_number,
        page_count=page_count,
        source_url="https://www.transfermarkt.com/wettbewerbe/europa",
        source_body_hash="5" * 64,
        competitions=(eligible, unknown),
        editions=editions,
    )


class _FakeClient:
    instances: list["_FakeClient"] = []

    def __init__(self, **kwargs):
        self.ledger = kwargs["traffic_ledger"]
        self.retry_budget = kwargs["retry_budget"]
        self.cache = kwargs["cache"]
        self.rate_limiter = kwargs.get("rate_limiter")
        self.fetch_calls: list[dict] = []
        self.retries = 0
        self.request_budget = None
        self.closed = False
        type(self).instances.append(self)

    def begin_request_scope(self, *, request_attempt_budget):
        self.request_budget = request_attempt_budget

    def get_traffic_stats(self):
        return {"retries": self.retries}

    def fetch(self, url, **kwargs):
        self.fetch_calls.append({"url": url, **kwargs})
        body = "<html><body><main>catalog</main></body></html>"
        self.ledger.observe_lease(
            "fake-lease",
            LeaseTrafficSnapshot(up_bytes=10, down_bytes=90),
        )
        self.ledger.record_attempt(
            entity="competition_registry",
            decoded_bytes=len(body.encode()),
            provider_up_bytes=10,
            provider_down_bytes=90,
            retry=False,
            duration_seconds=0.25,
        )
        self.cache[kwargs["cache_key"]] = {
            "cache_version": 1,
            "expires_at": 9999999999,
            "outcome": {},
        }
        return FetchOutcome(
            status=FetchStatus.OK,
            value=body,
            status_code=200,
            attempts=1,
            label="competition_registry",
            decoded_body_bytes=len(body.encode()),
            provider_metered_bytes=100,
            payload_hash=stable_payload_hash(body),
        )

    def close(self):
        self.closed = True


class _HttpZeroClient(_FakeClient):
    def fetch(self, url, **kwargs):
        self.fetch_calls.append({"url": url, **kwargs})
        self.ledger.observe_lease(
            "failed-lease",
            LeaseTrafficSnapshot(up_bytes=50, down_bytes=150),
        )
        for attempt in range(3):
            self.ledger.record_attempt(
                entity="competition_registry",
                decoded_bytes=0,
                provider_up_bytes=50 if attempt == 0 else 0,
                provider_down_bytes=150 if attempt == 0 else 0,
                retry=attempt > 0,
                duration_seconds=0.25,
            )
        self.retries = 2
        return FetchOutcome(
            status=FetchStatus.RETRY_EXHAUSTED,
            status_code=None,
            attempts=3,
            label="competition_registry",
            error="proxy preflight returned HTTP 0",
            provider_metered_bytes=200,
        )


class _FakeWriter:
    def __init__(self):
        self.calls: list[dict] = []

    def write_dataframe(self, frame, **kwargs):
        self.calls.append({"frame": frame.copy(), **kwargs})
        return f"iceberg.{kwargs['database']}.{kwargs['table']}"


class _FailingWriter(_FakeWriter):
    def write_dataframe(self, frame, **kwargs):
        super().write_dataframe(frame, **kwargs)
        raise RuntimeError("Iceberg append failed")


def _raw_args(tmp_path: Path, *, dry_run: bool, approval: bool) -> list[str]:
    values = [
        "--cycle-id",
        "tm-registry-20260711",
        "--dag-id",
        "dag_discover_transfermarkt",
        "--run-id",
        "manual__2026-07-11",
        "--task-id",
        "discover",
        "--proxy-control-url",
        "http://proxy-filter:8890",
        "--checkpoint",
        str(tmp_path / "checkpoint.json"),
        "--cache",
        str(tmp_path / "cache.json"),
        "--output-root",
        str(tmp_path / "manifests"),
        "--request-limit",
        "10",
        "--retry-limit",
        "2",
    ]
    if approval:
        values.extend(
            (
                "--paid-proxy-approval-packet",
                str(tmp_path / "paid-packet.json"),
                "--approval-journal",
                str(tmp_path / "journal.json"),
            )
        )
        if not dry_run:
            values.extend((
                "--production-write-approval-packet",
                str(tmp_path / "write-packet.json"),
            ))
    if dry_run:
        values.append("--dry-run")
    return values


def _approved_args(mod, tmp_path, monkeypatch, *, dry_run=False, **packet_changes):
    raw = _raw_args(tmp_path, dry_run=dry_run, approval=True)
    execution = (str(SCRIPT), *raw)
    values = {
        "packet_id": "tm-discovery-paid-approval",
        "action": "paid_proxy",
        "argv": execution,
        "byte_cap_bytes": 15_728_640,
        "byte_cap_mib": Decimal("15"),
        "request_limit": 10,
        "retry_limit": 2,
        "concurrency": 1,
        "expected_duration_seconds": 600,
        "affected_tables": (),
        "affected_files": (
            str((tmp_path / "checkpoint.json").resolve()),
            str((tmp_path / "cache.json").resolve()),
            str((tmp_path / "manifests").resolve()),
            str((tmp_path / "journal.json").resolve()),
        ),
        "stop_conditions": (
            "provider bytes reach 14680064",
            "HTTP status is 0, 404 or 5xx",
            "source hash or schema validation fails",
        ),
        "backup_commands": (("trino", "--execute", "SELECT 1"),),
        "rollback_commands": (("trino", "--execute", "SELECT 1"),),
    }
    values.update(packet_changes)
    paid_packet = ApprovalPacket(**values)
    (tmp_path / "paid-packet.json").write_text(
        json.dumps(paid_packet.payload()), encoding="utf-8"
    )
    journal = ApprovalJournal(tmp_path / "journal.json")
    expiry = datetime.now(timezone.utc) + timedelta(minutes=10)
    journal.issue(paid_packet, expires_at=expiry)
    journal.approve(paid_packet, presented_hash=paid_packet.packet_hash)
    monkeypatch.setenv(mod.PAID_PRESENTED_HASH_ENV, paid_packet.packet_hash)
    write_packet = None
    if not dry_run:
        write_values = dict(values)
        write_values.update(
            packet_id="tm-discovery-write-approval",
            action="production_write",
            affected_tables=mod.TARGET_TABLES,
        )
        write_packet = ApprovalPacket(**write_values)
        (tmp_path / "write-packet.json").write_text(
            json.dumps(write_packet.payload()), encoding="utf-8"
        )
        journal.issue(write_packet, expires_at=expiry)
        journal.approve(write_packet, presented_hash=write_packet.packet_hash)
        monkeypatch.setenv(
            mod.WRITE_PRESENTED_HASH_ENV, write_packet.packet_hash,
        )
    return (
        mod._parser().parse_args(raw), execution, paid_packet, write_packet,
        journal,
    )


def _discovery(*, call_fetch: bool, incomplete: bool = False):
    def run(*, fetch, checkpoint, traffic_ledger, clock):
        del traffic_ledger, clock
        if call_fetch:
            url = "https://www.transfermarkt.com/wettbewerbe/europa"
            outcome = fetch(url)
            checkpoint[url] = {
                "status": outcome.status.value,
                "status_code": outcome.status_code,
                "body": outcome.value,
                "payload_hash": outcome.payload_hash,
                "attempts": outcome.attempts,
                "decoded_body_bytes": outcome.decoded_body_bytes,
            }
        return (_page(page_count=2 if incomplete else 1),)

    return run


def _http_zero_discovery(*, fetch, checkpoint, traffic_ledger, clock):
    del checkpoint, traffic_ledger, clock
    outcome = fetch("https://www.transfermarkt.com/wettbewerbe/europa")
    raise RuntimeError(
        "required discovery page failed: "
        f"status={outcome.status.value}, http={outcome.status_code or 0}"
    )


def test_cached_dry_run_retains_unknown_but_never_emits_its_scope(tmp_path):
    mod = _load()
    (tmp_path / "checkpoint.json").write_text(
        json.dumps({"resume": {"status": "ok"}}), encoding="utf-8"
    )
    raw = _raw_args(tmp_path, dry_run=True, approval=False)
    args = mod._parser().parse_args(raw)

    result = mod.execute(
        args,
        execution_argv=(str(SCRIPT), *raw),
        discovery_fn=_discovery(call_fetch=False),
        lease_provider_factory=lambda url: object(),
        http_client_factory=_FakeClient,
        writer_factory=lambda: pytest.fail("dry-run created an Iceberg writer"),
        utcnow=lambda: NOW,
        monotonic=iter((10.0, 12.0)).__next__,
    )

    assert result.manifest["rows"] == {
        "competitions": 2,
        "competition_editions": 2,
    }
    assert result.manifest["classification_counts"] == {
        "eligible": 1,
        "unknown": 1,
    }
    assert result.manifest["blocked_competition_ids"] == ["UNK"]
    assert result.manifest["crawl_scope_count"] == 1
    assert result.manifest["crawl_scopes"][0]["competition_id"] == "GB1"
    assert result.manifest["writes"] == []
    assert result.manifest["traffic"]["requests"] == 0
    assert result.manifest["checkpoint_resume"]["resumed"] is True
    envelope = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
    assert envelope["manifest_hash"] == result.manifest_hash
    assert mod.stable_payload_hash(envelope["manifest"]) == result.manifest_hash
    assert result.manifest_hash in Path(result.manifest_path).name


def test_unapproved_paid_fetch_fails_before_client_io_or_write(tmp_path):
    mod = _load()
    _FakeClient.instances.clear()
    raw = _raw_args(tmp_path, dry_run=True, approval=False)
    args = mod._parser().parse_args(raw)

    with pytest.raises(mod.DiscoveryRunnerError, match="paid proxy I/O"):
        mod.execute(
            args,
            execution_argv=(str(SCRIPT), *raw),
            discovery_fn=_discovery(call_fetch=True),
            lease_provider_factory=lambda url: object(),
            http_client_factory=_FakeClient,
            writer_factory=lambda: pytest.fail("writer must not be created"),
            utcnow=lambda: NOW,
        )

    assert _FakeClient.instances[-1].fetch_calls == []


def test_production_requires_separate_write_approval_before_client(tmp_path):
    mod = _load()
    _FakeClient.instances.clear()
    raw = _raw_args(tmp_path, dry_run=False, approval=False)
    args = mod._parser().parse_args(raw)
    with pytest.raises(mod.DiscoveryRunnerError, match='paid_proxy packet'):
        mod.execute(
            args,
            execution_argv=(str(SCRIPT), *raw),
            discovery_fn=_discovery(call_fetch=True),
            lease_provider_factory=lambda url: object(),
            http_client_factory=_FakeClient,
            utcnow=lambda: NOW,
        )
    assert _FakeClient.instances == []


def test_approved_production_is_metered_and_writes_one_batch_per_table(
    tmp_path, monkeypatch
):
    mod = _load()
    _FakeClient.instances.clear()
    args, execution, paid_packet, write_packet, journal = _approved_args(
        mod, tmp_path, monkeypatch
    )
    writer = _FakeWriter()

    result = mod.execute(
        args,
        execution_argv=execution,
        discovery_fn=_discovery(call_fetch=True),
        lease_provider_factory=lambda url: object(),
        http_client_factory=_FakeClient,
        writer_factory=lambda: writer,
        utcnow=lambda: NOW,
        monotonic=iter((20.0, 23.5)).__next__,
    )

    assert journal.get(paid_packet.packet_hash).status == "consumed"
    assert write_packet is not None
    assert journal.get(write_packet.packet_hash).status == "consumed"
    assert len(writer.calls) == 2
    assert [call["table"] for call in writer.calls] == [
        "transfermarkt_competitions",
        "transfermarkt_competition_editions",
    ]
    assert all(call["add_metadata"] is False for call in writer.calls)
    assert all(call["mode"] == "append" for call in writer.calls)
    assert all(call["delete_filter"].endswith(f"'{SNAPSHOT_ID}'") for call in writer.calls)
    assert tuple(writer.calls[0]["frame"].columns) == mod.COMPETITION_COLUMNS
    assert tuple(writer.calls[1]["frame"].columns) == mod.EDITION_COLUMNS
    assert set(writer.calls[0]["frame"]["competition_id"]) == {"GB1", "UNK"}
    assert result.manifest["traffic"]["requests"] == 1
    assert result.manifest["traffic"]["provider_metered_bytes"] == 100
    assert result.manifest["traffic"]["by_entity"]["competition_registry"][
        "provider_mib"
    ] == pytest.approx(100 / 1024 / 1024, abs=1e-6)
    assert result.manifest["duration_seconds"] == 3.5
    assert _FakeClient.instances[-1].retry_budget == 2
    assert _FakeClient.instances[-1].ledger.snapshot()["retry_limit"] == 2
    assert _FakeClient.instances[-1].fetch_calls[0]["max_attempts"] == 3
    limiter = _FakeClient.instances[-1].rate_limiter
    assert limiter is not None
    assert limiter.config.max_requests == mod.REQUESTS_PER_MINUTE
    assert limiter.config.window_seconds == 60


def test_incomplete_reconciliation_happens_before_all_writes_and_closes_packet(
    tmp_path, monkeypatch
):
    mod = _load()
    args, execution, paid_packet, write_packet, journal = _approved_args(
        mod, tmp_path, monkeypatch
    )
    writer = _FakeWriter()

    with pytest.raises(Exception, match="incomplete pagination"):
        mod.execute(
            args,
            execution_argv=execution,
            discovery_fn=_discovery(call_fetch=True, incomplete=True),
            lease_provider_factory=lambda url: object(),
            http_client_factory=_FakeClient,
            writer_factory=lambda: writer,
            utcnow=lambda: NOW,
        )

    assert writer.calls == []
    assert journal.get(paid_packet.packet_hash).status == "failed"
    assert write_packet is not None
    assert journal.get(write_packet.packet_hash).status == "failed"
    with pytest.raises(Exception, match="cannot consume"):
        journal.consume(
            write_packet,
            presented_hash=write_packet.packet_hash,
            execution_argv=write_packet.argv,
        )
    persisted = json.loads((tmp_path / "checkpoint.json").read_text(encoding="utf-8"))
    assert "https://www.transfermarkt.com/wettbewerbe/europa" in persisted


def test_http_zero_writes_atomic_terminal_manifest_and_never_touches_bronze(
    tmp_path, monkeypatch
):
    mod = _load()
    _HttpZeroClient.instances.clear()
    args, execution, paid_packet, write_packet, journal = _approved_args(
        mod, tmp_path, monkeypatch
    )
    writer = _FakeWriter()

    with pytest.raises(RuntimeError, match="http=0") as raised:
        mod.execute(
            args,
            execution_argv=execution,
            discovery_fn=_http_zero_discovery,
            lease_provider_factory=lambda url: object(),
            http_client_factory=_HttpZeroClient,
            writer_factory=lambda: writer,
            utcnow=lambda: NOW,
            monotonic=iter((30.0, 34.0)).__next__,
        )

    assert writer.calls == []
    assert write_packet is not None
    assert journal.get(paid_packet.packet_hash).status == "failed"
    assert journal.get(write_packet.packet_hash).status == "failed"
    failure = raised.value.manifest
    assert failure["status"] == "failed"
    assert failure["cycle_id"] == "tm-registry-20260711"
    assert failure["run_id"] == "manual__2026-07-11"
    assert failure["scope"] == {
        "cycle_id": "tm-registry-20260711",
        "dag_id": "dag_discover_transfermarkt",
        "run_id": "manual__2026-07-11",
        "task_id": "discover",
        "scope_id": "tm-registry-20260711",
        "entity": "competition_registry",
    }
    assert failure["expected_entities"] == [
        "competitions",
        "competition_editions",
    ]
    assert failure["rows"] == {
        "competitions": None,
        "competition_editions": None,
    }
    assert failure["hashes"] == {
        "competitions": None,
        "competition_editions": None,
    }
    assert failure["authoritative_empty"] is False
    assert failure["not_applicable"] is False
    assert failure["source_status"] == "failed"
    assert failure["dq"] == {
        "status": "failed",
        "source_complete": False,
        "schema_validation": "failed",
        "classification_validation": "failed",
    }
    assert failure["write_status"] == {
        "status": "blocked_source_failure",
        "authorized": False,
        "attempted": False,
        "completed": False,
        "affected_tables": list(mod.TARGET_TABLES),
    }
    assert failure["writes"] == []
    assert failure["traffic"]["requests"] == 3
    assert failure["traffic"]["retries"] == 2
    assert failure["traffic"]["cache_hits"] == 0
    assert failure["traffic"]["cache_hit_rate"] == 0.0
    assert failure["traffic"]["decoded_bytes"] == 0
    assert failure["traffic"]["wire_bytes"] == 150
    assert failure["traffic"]["provider_metered_bytes"] == 200
    assert failure["duration_seconds"] == 4.0

    envelope = json.loads(
        Path(raised.value.manifest_path).read_text(encoding="utf-8")
    )
    assert envelope == {
        "manifest_hash": raised.value.manifest_hash,
        "manifest": failure,
    }
    assert mod.stable_payload_hash(failure) == raised.value.manifest_hash
    assert list((tmp_path / "manifests").rglob("*.tmp")) == []


def test_post_validation_write_failure_keeps_exact_row_hashes(
    tmp_path, monkeypatch
):
    mod = _load()
    args, execution, _, _, _ = _approved_args(mod, tmp_path, monkeypatch)
    writer = _FailingWriter()

    with pytest.raises(RuntimeError, match="Iceberg append failed") as raised:
        mod.execute(
            args,
            execution_argv=execution,
            discovery_fn=_discovery(call_fetch=True),
            lease_provider_factory=lambda url: object(),
            http_client_factory=_FakeClient,
            writer_factory=lambda: writer,
            utcnow=lambda: NOW,
            monotonic=iter((40.0, 45.0)).__next__,
        )

    snapshot = mod.reconcile_registry_pages((_page(),))
    competition_rows, edition_rows, _ = mod._flatten_snapshot(
        snapshot,
        cycle_id="tm-registry-20260711",
        fetched_at=NOW,
    )
    failure = raised.value.manifest
    assert failure["rows"] == {
        "competitions": 2,
        "competition_editions": 2,
    }
    assert failure["hashes"] == {
        "competitions": mod.stable_payload_hash(competition_rows),
        "competition_editions": mod.stable_payload_hash(edition_rows),
    }
    assert failure["source_status"] == "success"
    assert failure["dq"]["status"] == "passed"
    assert failure["write_status"]["status"] == "failed_or_partial"
    assert failure["authoritative_empty"] is False
    assert failure["not_applicable"] is False
    assert len(writer.calls) == 1


def test_approval_asset_drift_fails_before_proxy_and_state_mutation(
    tmp_path, monkeypatch
):
    mod = _load()
    _FakeClient.instances.clear()
    args, execution, _, _, _ = _approved_args(
        mod,
        tmp_path,
        monkeypatch,
        affected_tables=(mod.COMPETITIONS_TABLE,),
    )

    with pytest.raises(mod.DiscoveryRunnerError, match="table assets"):
        mod.execute(
            args,
            execution_argv=execution,
            discovery_fn=_discovery(call_fetch=True),
            lease_provider_factory=lambda url: object(),
            http_client_factory=_FakeClient,
            utcnow=lambda: NOW,
        )

    assert _FakeClient.instances == []
    assert not (tmp_path / "checkpoint.json").exists()


def _standing_policy_file(mod, tmp_path: Path, **overrides) -> Path:
    now = datetime.now(timezone.utc)
    value = {
        "policy_version": 1,
        "dag_id": "dag_discover_transfermarkt_registry",
        "approved_by": "sergeykuznetsov1995",
        "approved_at": (now - timedelta(days=1)).isoformat(),
        "expires_at": (now + timedelta(days=30)).isoformat(),
        "paid_proxy": {
            "byte_cap_bytes": mod.HARD_PROVIDER_BYTE_BUDGET,
            # The wrapper limits of the test argv (_raw_args), not the DAG's.
            "request_limit": 10,
            "retry_limit": 2,
            "concurrency": 1,
        },
        "production_write": {
            "byte_cap_bytes": 0,
            "request_limit": 0,
            "retry_limit": 0,
            "concurrency": 1,
        },
        "allowed_write_tables": sorted(mod.TARGET_TABLES),
    }
    value.update(overrides)
    path = tmp_path / "standing-registry-policy.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def _standing_args(mod, tmp_path: Path, *, dry_run: bool = False, **overrides):
    policy_path = _standing_policy_file(mod, tmp_path, **overrides)
    policy = load_standing_policy(policy_path)
    raw = _raw_args(tmp_path, dry_run=dry_run, approval=False)
    raw.extend(
        (
            "--standing-policy",
            str(policy_path),
            "--standing-policy-sha256",
            policy.policy_hash,
        )
    )
    return raw, policy


def _execute_standing(mod, tmp_path, raw, *, writer, discovery=None):
    return mod.execute(
        mod._parser().parse_args(raw),
        execution_argv=(str(SCRIPT), *raw),
        discovery_fn=discovery or _discovery(call_fetch=True),
        lease_provider_factory=lambda url: object(),
        http_client_factory=_FakeClient,
        writer_factory=lambda: writer,
        utcnow=lambda: NOW,
        monotonic=iter((20.0, 23.5)).__next__,
    )


def test_standing_policy_run_writes_bronze_without_journal(
    tmp_path, monkeypatch
):
    mod = _load()
    _FakeClient.instances.clear()
    monkeypatch.setenv(mod.STANDING_POLICY_ENV_GATE, "true")
    raw, policy = _standing_args(mod, tmp_path)
    writer = _FakeWriter()

    result = _execute_standing(mod, tmp_path, raw, writer=writer)

    assert [call["table"] for call in writer.calls] == [
        "transfermarkt_competitions",
        "transfermarkt_competition_editions",
    ]
    manifest = result.manifest
    assert manifest["approval_mode"] == "standing_policy"
    assert manifest["standing_policy_hash"] == policy.policy_hash
    assert manifest["fetched_at"] == NOW.isoformat()
    assert manifest["paid_proxy_approval_packet_hash"] is None
    assert manifest["production_write_approval_packet_hash"] is None
    assert not (tmp_path / "journal.json").exists()
    record = json.loads(
        (
            tmp_path
            / "manifests"
            / "tm-registry-20260711"
            / "standing-authorization.json"
        ).read_text(encoding="utf-8")
    )
    assert record == {
        "status": "complete",
        "cycle_id": "tm-registry-20260711",
        "run_id": "manual__2026-07-11",
        "manifest_hash": result.manifest_hash,
        "approval_mode": "standing_policy",
        "standing_policy": {
            "policy_hash": policy.policy_hash,
            "policy_version": 1,
        },
    }


def test_dry_run_standing_writes_no_authorization_record(
    tmp_path, monkeypatch
):
    # The record attests an authorized production write; a dry run writes
    # nothing to production, so leaving no record is the honest state.
    mod = _load()
    _FakeClient.instances.clear()
    monkeypatch.setenv(mod.STANDING_POLICY_ENV_GATE, "true")
    raw, policy = _standing_args(mod, tmp_path, dry_run=True)

    result = mod.execute(
        mod._parser().parse_args(raw),
        execution_argv=(str(SCRIPT), *raw),
        discovery_fn=_discovery(call_fetch=True),
        lease_provider_factory=lambda url: object(),
        http_client_factory=_FakeClient,
        writer_factory=lambda: pytest.fail("dry-run created an Iceberg writer"),
        utcnow=lambda: NOW,
        monotonic=iter((20.0, 23.5)).__next__,
    )

    assert result.manifest["approval_mode"] == "standing_policy"
    assert result.manifest["standing_policy_hash"] == policy.policy_hash
    assert result.manifest["write_status"]["status"] == "not_applicable"
    assert not (
        tmp_path
        / "manifests"
        / "tm-registry-20260711"
        / "standing-authorization.json"
    ).exists()


def test_standing_policy_requires_env_gate(tmp_path, monkeypatch):
    mod = _load()
    _FakeClient.instances.clear()
    monkeypatch.delenv("TM_STANDING_POLICY_ENABLED", raising=False)
    raw, _ = _standing_args(mod, tmp_path)

    with pytest.raises(
        mod.DiscoveryRunnerError,
        match="TM_STANDING_POLICY_ENABLED must be true",
    ):
        _execute_standing(
            mod,
            tmp_path,
            raw,
            writer=None,
            discovery=_discovery(call_fetch=True),
        )

    assert _FakeClient.instances == []
    assert not (tmp_path / "checkpoint.json").exists()


def test_standing_policy_sha_drift_fails_before_client_io(
    tmp_path, monkeypatch
):
    mod = _load()
    _FakeClient.instances.clear()
    monkeypatch.setenv(mod.STANDING_POLICY_ENV_GATE, "true")
    raw, _ = _standing_args(mod, tmp_path)
    raw[raw.index("--standing-policy-sha256") + 1] = "0" * 64

    with pytest.raises(
        mod.DiscoveryRunnerError, match="differs from the pinned sha256"
    ) as raised:
        _execute_standing(mod, tmp_path, raw, writer=None)

    assert _FakeClient.instances == []
    assert not (tmp_path / "checkpoint.json").exists()
    # The evidence names the mode the run *attempted*, even though the
    # enforcement failed before any authorization object existed.
    assert raised.value.manifest["approval_mode"] == "standing_policy"
    assert raised.value.manifest["standing_policy_hash"] is None
    assert raised.value.manifest["traffic"]["requests"] == 0


@pytest.mark.parametrize(
    "paid_override",
    [
        {"byte_cap_bytes": 16 * 1024 * 1024},
        {"request_limit": 11},
        {"retry_limit": 3},
        {"concurrency": 2},
    ],
)
def test_standing_policy_caps_must_match_wrapper_argv(
    tmp_path, monkeypatch, paid_override
):
    mod = _load()
    _FakeClient.instances.clear()
    monkeypatch.setenv(mod.STANDING_POLICY_ENV_GATE, "true")
    paid = {
        "byte_cap_bytes": mod.HARD_PROVIDER_BYTE_BUDGET,
        "request_limit": 10,
        "retry_limit": 2,
        "concurrency": 1,
    }
    paid.update(paid_override)
    raw, _ = _standing_args(mod, tmp_path, paid_proxy=paid)

    with pytest.raises(
        mod.DiscoveryRunnerError, match="caps differ from discovery limits"
    ):
        _execute_standing(mod, tmp_path, raw, writer=None)

    assert _FakeClient.instances == []


def test_standing_policy_expired_fails_before_client_io(tmp_path, monkeypatch):
    mod = _load()
    _FakeClient.instances.clear()
    monkeypatch.setenv(mod.STANDING_POLICY_ENV_GATE, "true")
    now = datetime.now(timezone.utc)
    raw, _ = _standing_args(
        mod,
        tmp_path,
        approved_at=(now - timedelta(days=2)).isoformat(),
        expires_at=(now - timedelta(days=1)).isoformat(),
    )

    with pytest.raises(Exception, match="expired"):
        _execute_standing(mod, tmp_path, raw, writer=None)

    assert _FakeClient.instances == []


def test_standing_policy_wrong_dag_id_fails_before_client_io(
    tmp_path, monkeypatch
):
    mod = _load()
    _FakeClient.instances.clear()
    monkeypatch.setenv(mod.STANDING_POLICY_ENV_GATE, "true")
    raw, _ = _standing_args(mod, tmp_path, dag_id="dag_ingest_transfermarkt")

    with pytest.raises(mod.DiscoveryRunnerError, match="dag_id mismatch"):
        _execute_standing(mod, tmp_path, raw, writer=None)

    assert _FakeClient.instances == []


def test_standing_policy_missing_bronze_table_fails_before_client_io(
    tmp_path, monkeypatch
):
    mod = _load()
    _FakeClient.instances.clear()
    monkeypatch.setenv(mod.STANDING_POLICY_ENV_GATE, "true")
    raw, _ = _standing_args(
        mod, tmp_path, allowed_write_tables=[mod.COMPETITIONS_TABLE]
    )

    with pytest.raises(mod.DiscoveryRunnerError, match="omits write tables"):
        _execute_standing(mod, tmp_path, raw, writer=None)

    assert _FakeClient.instances == []


def test_standing_and_one_shot_flags_are_mutually_exclusive(
    tmp_path, monkeypatch
):
    mod = _load()
    _FakeClient.instances.clear()
    monkeypatch.setenv(mod.STANDING_POLICY_ENV_GATE, "true")
    raw, _ = _standing_args(mod, tmp_path)
    raw.extend(("--approval-journal", str(tmp_path / "journal.json")))

    with pytest.raises(
        mod.DiscoveryRunnerError, match="mutually exclusive"
    ):
        _execute_standing(mod, tmp_path, raw, writer=None)

    assert _FakeClient.instances == []


@pytest.mark.parametrize(
    "dropped", ["--standing-policy", "--standing-policy-sha256"]
)
def test_standing_policy_needs_both_flags(tmp_path, monkeypatch, dropped):
    mod = _load()
    _FakeClient.instances.clear()
    monkeypatch.setenv(mod.STANDING_POLICY_ENV_GATE, "true")
    raw, _ = _standing_args(mod, tmp_path)
    index = raw.index(dropped)
    del raw[index:index + 2]

    with pytest.raises(mod.DiscoveryRunnerError, match="is required"):
        _execute_standing(mod, tmp_path, raw, writer=None)

    assert _FakeClient.instances == []


def test_failed_rerun_does_not_clobber_standing_authorization(
    tmp_path, monkeypatch
):
    mod = _load()
    _FakeClient.instances.clear()
    monkeypatch.setenv(mod.STANDING_POLICY_ENV_GATE, "true")
    raw, _ = _standing_args(mod, tmp_path)
    _execute_standing(mod, tmp_path, raw, writer=_FakeWriter())
    record_path = (
        tmp_path
        / "manifests"
        / "tm-registry-20260711"
        / "standing-authorization.json"
    )
    original = record_path.read_text(encoding="utf-8")

    drifted = list(raw)
    drifted[drifted.index("--standing-policy-sha256") + 1] = "0" * 64
    with pytest.raises(
        mod.DiscoveryRunnerError, match="differs from the pinned sha256"
    ) as raised:
        _execute_standing(mod, tmp_path, drifted, writer=None)

    assert record_path.read_text(encoding="utf-8") == original
    assert Path(raised.value.manifest_path) != record_path
    assert Path(raised.value.manifest_path).is_file()


def test_corrupt_checkpoint_fails_closed_without_proxy_io(tmp_path):
    mod = _load()
    _FakeClient.instances.clear()
    (tmp_path / "checkpoint.json").write_text("{broken", encoding="utf-8")
    raw = _raw_args(tmp_path, dry_run=True, approval=False)
    args = mod._parser().parse_args(raw)

    with pytest.raises(mod.DiscoveryRunnerError, match="unreadable") as raised:
        mod.execute(
            args,
            execution_argv=(str(SCRIPT), *raw),
            discovery_fn=_discovery(call_fetch=False),
            lease_provider_factory=lambda url: object(),
            http_client_factory=_FakeClient,
            utcnow=lambda: NOW,
        )

    assert _FakeClient.instances == []
    assert raised.value.manifest["status"] == "failed"
    assert raised.value.manifest["source_status"] == "not_started"
    assert raised.value.manifest["traffic"]["requests"] == 0
    assert raised.value.manifest["traffic"]["by_entity"][
        "competition_registry"
    ]["provider_bytes"] == 0
    assert Path(raised.value.manifest_path).is_file()
