"""Offline contract for one bounded Transfermarkt registry discovery cycle.

Unlike tests/unit/scripts/test_run_transfermarkt_discovery.py (fake discovery
functions; mostly dry runs, though some writer tests run non-dry), these
tests drive the real chain end to end offline:

* the real ``execute`` runner with resumable portions (standing policy);
* the real ``DiscoveryContinuation`` cohort marker on durable ``tmp_path``
  storage (pause -> settled resume), including the setup-failure window where
  a premature ``mark_running`` would strand the cohort in ``running``;
* the real ``TransfermarktHttpClient`` constructor, whose shared-ledger
  contract rejects a ``retry_budget`` of *remaining* retries — only network
  I/O is stubbed, by subclassing fetch over the synthetic fixture payloads;
* the real ``discover_competition_registry`` parser (daily mode) over the
  synthetic tmapi regulation fixtures, checkpointed in tmp_path;

The scope is daily mode only: full mode is neither exercised nor claimed
here.
* a synthetic in-memory Bronze writer (no external storage).

Canonical publication is *not* exercised here: it belongs to the publish DAG
and needs the warehouse/runtime, which is unavailable offline; that coverage
stays in tests/unit/dags/test_dag_discover_transfermarkt_registry.py.  These
tests therefore stop at the honest ``source_complete`` marker with a bound
``final_manifest`` and only prove that a mismatched publication marker is
refused — no marker is faked to claim full publication.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from dags.utils.transfermarkt_discovery_continuation import (
    cohort_day,
    identity_for,
    mark_published,
    read_job,
    select_job,
)
from scrapers.transfermarkt.client import TransfermarktHttpClient
from scrapers.transfermarkt.discovery import BASE_URL, PreviousRegistry
from scrapers.transfermarkt.models import (
    FetchOutcome,
    FetchStatus,
    LeaseTrafficSnapshot,
    stable_payload_hash,
)
from scrapers.transfermarkt.tmapi import competition_regulation_url


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "transfermarkt" / "discovery"
NOW = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
PREVIOUS_SNAPSHOT_ID = "tm-discovery-" + "c" * 24
FAC_URL = competition_regulation_url("FAC")
RSK1_URL = competition_regulation_url("RSK1")
REGULATION_PAYLOADS = {
    competition_regulation_url(competition_id): json.loads(
        (FIXTURES / f"regulation_{competition_id.lower()}.json").read_text(
            encoding="utf-8"
        )
    )
    for competition_id in ("FAC", "RSK1")
}


def _helpers():
    """Import the existing runner test module without pytest collection."""
    name = "tm_discovery_runner_test_helpers"
    path = Path(__file__).with_name("test_run_transfermarkt_discovery.py")
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _FixtureRegulationClient(TransfermarktHttpClient):
    """Real constructor/ledger contract; only the network fetch is stubbed.

    Serving the synthetic regulation fixtures through ``fetch`` keeps the
    shared-ledger accounting (requests, retries, provider bytes) real, so a
    runner that passed *remaining* retries as ``retry_budget`` is rejected by
    the real constructor instead of a mock.
    """

    instances: list["_FixtureRegulationClient"] = []
    spend_retry = False
    advance_clock = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.kwargs = kwargs
        self.fetch_calls: list[dict] = []
        self.request_budget = None
        self.closed = False
        type(self).instances.append(self)

    def begin_request_scope(self, *, request_attempt_budget):
        self.request_budget = request_attempt_budget

    def fetch(self, url, **kwargs):
        self.fetch_calls.append({"url": url, **kwargs})
        if url not in REGULATION_PAYLOADS:
            raise AssertionError(f"chain must only buy fixture URLs: {url}")
        payload = REGULATION_PAYLOADS[url]
        body = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        ledger = self._traffic_ledger
        lease = f"fixture-lease-{len(self.fetch_calls)}"
        ledger.observe_lease(lease, LeaseTrafficSnapshot(up_bytes=10, down_bytes=90))
        ledger.record_attempt(
            entity="competition_registry",
            decoded_bytes=len(body.encode()),
            provider_up_bytes=10,
            provider_down_bytes=90,
            retry=False,
            duration_seconds=0.0,
        )
        attempts = 1
        if type(self).spend_retry:
            # The first bounded portion settles with one cumulative retry spent.
            type(self).spend_retry = False
            ledger.record_attempt(
                entity="competition_registry",
                decoded_bytes=0,
                provider_up_bytes=0,
                provider_down_bytes=0,
                retry=True,
                duration_seconds=0.0,
            )
            attempts = 2
        if type(self).advance_clock is not None:
            type(self).advance_clock()
        return FetchOutcome(
            status=FetchStatus.OK,
            value=payload,
            status_code=200,
            attempts=attempts,
            label="competition_registry",
            decoded_body_bytes=len(body.encode()),
            provider_metered_bytes=100,
            payload_hash=stable_payload_hash(payload),
        )

    def close(self):
        self.closed = True


class _RecordingWriter:
    """Synthetic Bronze writer: records what the runner would have appended."""

    def __init__(self):
        self.calls: list[dict] = []

    def write_dataframe(self, frame, **kwargs):
        self.calls.append({"frame": frame.copy(), **kwargs})
        return f"iceberg.{kwargs['database']}.{kwargs['table']}"


def _evidence(*, competition_type: str, season_format: str, source_url: str):
    return json.dumps(
        [
            {
                "origin": "source_page",
                "precedence": 2,
                "signals": {
                    "competition_type": competition_type,
                    "team_type": "club",
                    "age_category": "senior",
                },
                "source_field": "section_label",
                "source_url": BASE_URL + "/wettbewerbe/europa",
                "source_value": "National competitions",
            },
            {
                "origin": "structured",
                "precedence": 0,
                "signals": {"gender": "men"},
                "source_field": "transfermarkt_taxonomy",
                "source_url": BASE_URL + "/wettbewerbe/europa",
                "source_value": "main men's competitions taxonomy",
            },
            {
                "origin": "structured",
                "precedence": 0,
                "signals": {"season_format": season_format},
                "source_field": "edition_selector",
                "source_url": source_url,
                "source_value": "2025,2024",
            },
        ]
    )


def _competition_row(
    *,
    competition_id: str,
    slug: str,
    name: str,
    route: str,
    competition_type: str,
    season_format: str,
) -> dict:
    source_url = f"{BASE_URL}/{slug}/startseite/{route}/{competition_id}"
    return {
        "competition_id": competition_id,
        "slug": slug,
        "name": name,
        "country": "England" if competition_id == "FAC" else "Korea, South",
        "confederation": "UEFA" if competition_id == "FAC" else "AFC",
        "competition_type": competition_type,
        "gender": "men",
        "team_type": "club",
        "age_category": "senior",
        "season_format": season_format,
        "active": True,
        "source_url": source_url,
        "discovered_at": NOW - timedelta(days=1),
        "canonical_competition_id": None,
        "classification_evidence": _evidence(
            competition_type=competition_type,
            season_format=season_format,
            source_url=source_url,
        ),
        "source_body_hash": "a" * 64,
        "parser_revision": "tm-html-discovery-v6",
        "schema_revision": "1",
    }


def _edition_row(
    competition: dict,
    *,
    edition_id: str,
    label: str,
    current: bool,
    body_hash: str,
) -> dict:
    return {
        "competition_id": competition["competition_id"],
        "edition_id": edition_id,
        "edition_label": label,
        "canonical_season": label,
        "season_format": competition["season_format"],
        "start_date": None,
        "end_date": None,
        "active": True,
        "is_current": current,
        "participant_count": None,
        "participant_hash": None,
        "source_url": f"{competition['source_url']}/saison_id/{edition_id}",
        "discovered_at": NOW - timedelta(days=1),
        "source_body_hash": body_hash,
        "parser_revision": "tm-html-discovery-v6",
        "schema_revision": "1",
    }


def _previous_registry() -> PreviousRegistry:
    """The canonical snapshot the daily chain refreshes (synthetic rows)."""
    fac = _competition_row(
        competition_id="FAC",
        slug="fa-cup",
        name="FA Cup",
        route="pokalwettbewerb",
        competition_type="domestic_cup",
        season_format="split_year",
    )
    rsk1 = _competition_row(
        competition_id="RSK1",
        slug="k-league-1",
        name="K League 1",
        route="wettbewerb",
        competition_type="domestic_league",
        season_format="single_year",
    )
    editions = [
        _edition_row(fac, edition_id="2025", label="25/26", current=True,
                     body_hash="b" * 64),
        _edition_row(rsk1, edition_id="2025", label="2026", current=True,
                     body_hash="c" * 64),
        _edition_row(rsk1, edition_id="2024", label="2025", current=False,
                     body_hash="d" * 64),
    ]
    return PreviousRegistry.from_rows(
        PREVIOUS_SNAPSHOT_ID, [fac, rsk1], editions
    )


def _cycle_args(mod, helpers, tmp_path, monkeypatch):
    """Standing-policy daily portion args plus its settled cohort marker."""
    monkeypatch.setenv(mod.STANDING_POLICY_ENV_GATE, "true")
    raw, _policy = helpers._standing_args(mod, tmp_path, dry_run=False)
    raw += [
        "--portion-seconds", "1200",
        "--deadline-at", (NOW + timedelta(minutes=55)).isoformat(),
        "--job-path", str(tmp_path / "cohorts" / "active.json"),
        "--mode", "daily",
        "--previous-snapshot-id", PREVIOUS_SNAPSHOT_ID,
    ]
    args = mod._parser().parse_args(raw)
    job = select_job(
        Path(args.job_path),
        identity_for(
            previous_snapshot_id=PREVIOUS_SNAPSHOT_ID,
            mode="daily",
            policy_hash=args.standing_policy_sha256,
            request_limit=args.request_limit,
            retry_limit=args.retry_limit,
            byte_cap_bytes=args.byte_cap_bytes,
            day=cohort_day(NOW),
            dry_run=args.dry_run,
        ),
        now=NOW,
        write=mod._atomic_json_write,
    )
    args.checkpoint = job["checkpoint_path"]
    return args, raw


def _execute(mod, helpers, args, raw, clock, now, **overrides):
    options = dict(
        execution_argv=(str(helpers.SCRIPT), *raw),
        previous_reader=lambda _snapshot_id: _previous_registry(),
        lease_provider_factory=lambda _url: object(),
        utcnow=lambda: now,
        monotonic=lambda: clock[0],
    )
    options.update(overrides)
    return mod.execute(args, **options)


def _first_portion(mod, helpers, args, raw, clock):
    """Run portion 1: one paid regulation, one spent retry, then the pause."""
    _FixtureRegulationClient.instances.clear()
    _FixtureRegulationClient.spend_retry = True
    _FixtureRegulationClient.advance_clock = staticmethod(
        lambda: clock.__setitem__(0, 1140)
    )
    try:
        return mod.execute(
            args,
            execution_argv=(str(helpers.SCRIPT), *raw),
            previous_reader=lambda _snapshot_id: _previous_registry(),
            lease_provider_factory=lambda _url: object(),
            http_client_factory=_FixtureRegulationClient,
            writer_factory=lambda: pytest.fail(
                "a bounded incomplete portion must not write Bronze"
            ),
            utcnow=lambda: NOW,
            monotonic=lambda: clock[0],
        )
    finally:
        _FixtureRegulationClient.advance_clock = None


def test_pause_setup_failure_resume_completes_real_daily_cycle(
    tmp_path, monkeypatch
):
    helpers = _helpers()
    mod = helpers._load()
    args, raw = _cycle_args(mod, helpers, tmp_path, monkeypatch)
    clock = [0.0]
    job_path = Path(args.job_path)

    # Portion 1: the real daily parser buys the first regulation (one retry
    # spent), then the portion deadline pauses the run with no Bronze write.
    paused = _first_portion(mod, helpers, args, raw, clock)
    assert paused.manifest["status"] == "paused"
    assert paused.manifest["rows"]["competitions"] is None
    assert paused.manifest["dq"]["source_complete"] is False
    assert paused.manifest["traffic"]["requests"] == 2
    assert paused.manifest["traffic"]["retries"] == 1
    assert paused.manifest["traffic"]["provider_metered_bytes"] == 100
    first_client = _FixtureRegulationClient.instances[-1]
    assert first_client.closed
    assert first_client.fetch_calls[0]["url"] == FAC_URL
    # The real constructor received the total shared limit, never a remainder.
    assert first_client.kwargs["retry_budget"] == args.retry_limit
    saved = read_job(job_path)
    assert saved["status"] == "paused"
    assert saved["portion"] == 1
    assert saved["traffic"]["requests"] == 2
    assert saved["traffic"]["retries"] == 1
    job_id = saved["job_id"]
    started_at = saved["started_at"]
    checkpoint_path = saved["checkpoint_path"]
    failures_path = Path(checkpoint_path).with_suffix(".failures.json")
    assert failures_path.exists()  # the paid FAC answer is settled, not rebought
    # The pause fires in the runner's check_portion after fetch stored the
    # FAC outcome in checkpoint.failures.json but before the parser writes
    # checkpoint.json, so the purchased answer is recorded from the
    # failures file, not the (not yet written) parser checkpoint.
    failures_fac_entry = json.loads(
        failures_path.read_text(encoding="utf-8")
    )[FAC_URL]

    # Attempt 2: client setup fails before paid work.  The cohort must keep
    # its settled 'paused' state (a premature mark_running would strand it in
    # 'running') and the failure evidence keeps the restored accounting.
    args.run_id = "attempt-two"
    args.cycle_id = "tm-registry-attempt-two"
    args.deadline_at = (NOW + timedelta(hours=4, minutes=55)).isoformat()
    clock[0] = 0

    def broken_factory(**_kwargs):
        raise RuntimeError("client setup boom")

    with pytest.raises(RuntimeError, match="client setup boom") as caught:
        _execute(
            mod, helpers, args, raw, clock, NOW + timedelta(hours=4),
            http_client_factory=broken_factory,
            writer_factory=lambda: pytest.fail("setup failure must not write"),
        )
    saved = read_job(job_path)
    assert saved["status"] == "paused"
    assert saved["job_id"] == job_id
    failure = caught.value.manifest
    assert failure["status"] == "failed"
    assert failure["traffic"]["requests"] == 2
    assert failure["traffic"]["retries"] == 1
    assert failure["traffic"]["provider_metered_bytes"] == 100
    assert failure["write_status"]["attempted"] is False

    # Attempt 3: same identity and checkpoint, dry_run=False.  The settled
    # FAC answer is replayed instead of repurchased; only RSK1 is bought.
    args.run_id = "attempt-three"
    args.cycle_id = "tm-registry-attempt-three"
    clock[0] = 0
    writer = _RecordingWriter()
    complete = _execute(
        mod, helpers, args, raw, clock, NOW + timedelta(hours=4),
        http_client_factory=_FixtureRegulationClient,
        writer_factory=lambda: writer,
    )
    assert complete.manifest["status"] == "success"
    assert complete.manifest["dry_run"] is False
    assert complete.manifest["mode"] == "daily"
    assert complete.manifest["previous_snapshot_id"] == PREVIOUS_SNAPSHOT_ID
    assert complete.manifest["dq"]["source_complete"] is True
    assert complete.manifest["write_status"]["status"] == "complete"
    # Cumulative accounting: nothing was rebought, the spent retry carried.
    assert complete.manifest["traffic"]["requests"] == 3
    assert complete.manifest["traffic"]["retries"] == 1
    assert complete.manifest["traffic"]["provider_metered_bytes"] == 200
    resumed_client = _FixtureRegulationClient.instances[-1]
    assert [call["url"] for call in resumed_client.fetch_calls] == [RSK1_URL]
    assert resumed_client.kwargs["retry_budget"] == args.retry_limit
    assert (
        resumed_client.kwargs["retry_budget"]
        == resumed_client._traffic_ledger.retry_limit
    )
    assert resumed_client.request_budget == args.request_limit - 2

    # Real discovery over the synthetic regulations produced complete rows.
    assert complete.manifest["rows"] == {
        "competitions": 2,
        "competition_editions": 12 + 7,
    }
    assert [call["table"] for call in writer.calls] == [
        "transfermarkt_competitions",
        "transfermarkt_competition_editions",
    ]
    competitions = writer.calls[0]["frame"]
    editions = writer.calls[1]["frame"]
    assert set(competitions["competition_id"]) == {"FAC", "RSK1"}
    assert len(editions) == 12 + 7
    expected_edition_keys = {
        (entry["competitionId"], str(entry["season"]["id"]))
        for payload in REGULATION_PAYLOADS.values()
        for entry in payload["data"]
    }
    assert {
        (row["competition_id"], str(row["edition_id"]))
        for row in editions.to_dict("records")
    } == expected_edition_keys
    for competition_id in ("FAC", "RSK1"):
        current = editions[
            (editions["competition_id"] == competition_id) & editions["current"]
        ]
        assert len(current) == 1
    assert set(competitions["registry_snapshot_id"]) == {
        complete.manifest["snapshot_id"]
    }

    # Same cohort identity and checkpoint across portions; the completed
    # marker is bound to the exact final manifest on disk (source_complete,
    # not a faked publication).
    saved = read_job(job_path)
    assert saved["job_id"] == job_id
    assert saved["started_at"] == started_at
    assert saved["checkpoint_path"] == checkpoint_path
    assert saved["identity"]["previous_snapshot_id"] == PREVIOUS_SNAPSHOT_ID
    assert saved["identity"]["dry_run"] is False
    assert saved["status"] == "source_complete"
    final = saved["final_manifest"]
    assert final["path"] == complete.manifest_path
    assert final["hash"] == complete.manifest_hash
    assert final["snapshot_id"] == complete.manifest["snapshot_id"]
    assert final["cycle_id"] == args.cycle_id
    envelope = json.loads(Path(complete.manifest_path).read_text(encoding="utf-8"))
    assert envelope["manifest_hash"] == complete.manifest_hash
    checkpointed = json.loads(Path(checkpoint_path).read_text(encoding="utf-8"))
    assert {FAC_URL, RSK1_URL} <= set(checkpointed)
    final_failures = json.loads(failures_path.read_text(encoding="utf-8"))
    assert final_failures[FAC_URL] == failures_fac_entry

    # Publication is a separate DAG concern; a mismatched marker is refused.
    with pytest.raises(ValueError, match="does not match"):
        mark_published(
            job_path,
            manifest_hash="0" * 64,
            snapshot_id=complete.manifest["snapshot_id"],
            now=NOW + timedelta(hours=5),
            write=mod._atomic_json_write,
        )
    assert read_job(job_path)["status"] == "source_complete"


def test_expired_paused_cohort_is_rejected_without_io(tmp_path, monkeypatch):
    helpers = _helpers()
    mod = helpers._load()
    args, raw = _cycle_args(mod, helpers, tmp_path, monkeypatch)
    clock = [0.0]
    paused = _first_portion(mod, helpers, args, raw, clock)
    assert paused.manifest["status"] == "paused"

    job_path = Path(args.job_path)
    tracked = [job_path, Path(args.checkpoint)]
    failures = Path(args.checkpoint).with_suffix(".failures.json")
    tracked.append(failures)
    before = {path: path.read_bytes() for path in tracked if path.exists()}

    # A cohort at least a day old never renews budget: the refusal happens
    # before the lease provider, client or writer are even constructed.
    args.run_id = "expired-attempt"
    args.cycle_id = "tm-registry-expired"
    args.deadline_at = (NOW + timedelta(days=1, minutes=55)).isoformat()
    with pytest.raises(ValueError, match="expired"):
        _execute(
            mod, helpers, args, raw, clock, NOW + timedelta(days=1),
            lease_provider_factory=lambda _url: pytest.fail(
                "an expired cohort must not acquire a proxy lease"
            ),
            http_client_factory=lambda **_kw: pytest.fail(
                "an expired cohort must not construct a client"
            ),
            writer_factory=lambda: pytest.fail(
                "an expired cohort must not write"
            ),
        )
    for path, content in before.items():
        assert path.read_bytes() == content
    assert read_job(job_path)["status"] == "paused"
