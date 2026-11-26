"""Stage an exact cycle from a local copy of the ordinary Transfermarkt raw journal."""

import gzip
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
from pyarrow import fs

from dags.scripts import run_transfermarkt_bronze_cycle as cycle
from dags.scripts import stage_transfermarkt_bronze_cycle as stage
from scrapers.transfermarkt.raw_store import RawResponseStore, RawStoreError

pytestmark = pytest.mark.unit
FIXTURES = Path("tests/fixtures/transfermarkt/stage_a_rewrite")
CYCLE = "scheduled__2025-11-02T04:00:00+00:00"
START = datetime(2025, 11, 2, 4, tzinfo=timezone.utc)
AT = "2025-11-02T04:02:00+00:00"
URI = "s3://warehouse/raw/transfermarkt"


def _json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    return path


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


@pytest.fixture
def batch(tmp_path):
    source = next(row for row in json.loads((FIXTURES / "manifest.json").read_bytes())["fixtures"]
                  if row["endpoint"] == "coach_profile")
    source_record = json.loads((FIXTURES / source["capture_file"]).read_bytes())
    url = source_record["url"]
    slug, _, _, coach_id = urlsplit(url).path.strip("/").split("/")
    scope = {"scope_id": "one", "league": "JAM1", "season": "2627", "edition_id": "2026",
             "coaches": [f"{coach_id}:{slug}"]}
    inventory = {"cycle_id": CYCLE, "registry_snapshot_id": "registry-v1", "scopes": [scope]}
    store = RawResponseStore(fs.LocalFileSystem(), str(tmp_path / "archived-raw"), uri_prefix=URI)
    body = gzip.decompress((FIXTURES / source["blob_file"]).read_bytes())

    def response(**changes):
        raw = store.store_attempt(**({"body": body, "url": url, "cycle_id": CYCLE, "scope_id": "one",
                                      "status_code": 200, "headers": {}, "fetched_at": AT,
                                      "endpoint": "coach_profile", "attempt": 1} | changes))
        return store.store_response_envelope(raw)

    root = tmp_path / "stage"
    return SimpleNamespace(root=root, tmp=tmp_path, plan=_json(tmp_path / "source.json", inventory),
                           inventory=inventory, store=store, response=response, url=url,
                           traffic=_json(tmp_path / "traffic.json", {"request_attempts": 1,
                               "decoded_response_body_bytes": len(body), "provider_metered_bytes": None}),
                           directory=cycle.cycle_directory(root, CYCLE))


def _stage(batch):
    return stage.stage_plan(batch.root, CYCLE, batch.plan)


def _capture(batch, entries):
    journal = _json(batch.tmp / "journal.json", entries)
    return stage.stage_captures(batch.root, CYCLE, [journal], store=batch.store, started_at=START, traffic=batch.traffic)


def test_two_stage_cli_accepts_archive_objects_and_ids_and_consumes_exact_plan(batch, monkeypatch):
    # The mirror retains the production archive's original S3 URIs. No S3 adapter is needed.
    monkeypatch.setattr(RawResponseStore, "from_uri", lambda *_: pytest.fail("opened remote store"))
    monkeypatch.setattr(cycle.cli, "_write_options", lambda *_: pytest.fail("started a writer"))
    batch.inventory["scopes"].append(dict(batch.inventory["scopes"][0], scope_id="two"))
    _json(batch.plan, batch.inventory)
    base = ["--root", str(batch.root), "--cycle-id", CYCLE]
    assert stage.main(["plan", *base, "--inventory", str(batch.plan)]) == 0
    published = (batch.directory / "plan.json").read_bytes()
    assert published == _canonical(batch.inventory) and published != batch.plan.read_bytes()
    assert not (batch.directory / "captures.json").exists()
    first, second = batch.response(), batch.response(scope_id="two")
    journal1 = _json(batch.tmp / "one.json", asdict(first))
    journal2 = _json(batch.tmp / "two.json", {"raw_attempts": [second.envelope_id], "network_fetches": 1})
    _json(batch.traffic, {"request_attempts": 2, "run_id": CYCLE})
    args = ["captures", *base, "--raw-store-uri", URI, "--raw-mirror", batch.store.root,
            "--started-at", START.isoformat(), "--traffic", str(batch.traffic), "--attempts"]
    assert stage.main([*args, str(journal1), str(journal2)]) == 0
    index = batch.directory / "captures.json"
    assert json.loads(index.read_bytes()) == {
        "cycle_id": CYCLE, "plan_sha256": hashlib.sha256(published).hexdigest(),
        "envelopes_by_scope": {"one": [first.envelope_id], "two": [second.envelope_id]},
    }
    original = index.stat()
    assert stage.main([*args, str(journal2), str(journal1)]) == 0
    assert (index.stat().st_ino, index.stat().st_mtime_ns) == (original.st_ino, original.st_mtime_ns)
    universe, scopes = cycle._load_cycle(batch.directory, CYCLE)
    assert universe == "registry-v1"
    assert [s.envelopes for s in scopes] == [(first.envelope_id,), (second.envelope_id,)]


@pytest.mark.parametrize("first_publisher", ["scheduled", "standalone"])
def test_registry_retains_source_bytes_and_both_planners_reuse_one_canonical_plan(batch, monkeypatch, first_publisher):
    monkeypatch.delenv("TM_CORE_INVENTORY_SHA256", raising=False)
    started = datetime.now(timezone.utc)
    snapshot = {"scopes": batch.inventory["scopes"], "valid_from": (started - timedelta(days=1)).isoformat(),
                "valid_until": (started + timedelta(days=1)).isoformat()}
    source = _json(batch.tmp / "registry-input.json", snapshot)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    registered = batch.root / "registry" / f"{digest}.json"
    args = ["registry", "--root", str(batch.root), "--inventory", str(source)]
    assert stage.main(args) == 0 and registered.read_bytes() == source.read_bytes()
    original = registered.stat()
    assert stage.main(args) == 0
    assert (registered.stat().st_ino, registered.stat().st_mtime_ns) == (original.st_ino, original.st_mtime_ns)
    assert not batch.directory.exists() and "TM_CORE_INVENTORY_SHA256" not in os.environ

    monkeypatch.setenv("TM_CORE_INVENTORY_SHA256", digest)
    expected = dict(batch.inventory, registry_snapshot_id=digest)
    _json(batch.plan, expected)
    def scheduled():
        return cycle.prepare_scheduled_plan(batch.root, cycle_id=CYCLE, started_at=started)
    path = scheduled() if first_publisher == "scheduled" else _stage(batch)
    before = path.stat()
    batch.plan.write_text(json.dumps(dict(reversed(expected.items())), indent=6))
    assert _stage(batch) == scheduled() == path
    assert path.read_bytes() == _canonical(expected)
    assert (path.stat().st_ino, path.stat().st_mtime_ns) == (before.st_ino, before.st_mtime_ns)


@pytest.mark.parametrize("changes", [
    {"valid_from": "2025-11-02T04:00:00"}, {"valid_until": "2025-11-04"}, {"valid_from": None},
    {"valid_from": "invalid"}, {"valid_until": "2025-11-02T04:00:00+00:00"},
    {"valid_until": "2025-11-02T03:00:00+00:00"}, {"extra": "field"}, {"scopes": []},
])
def test_invalid_registry_is_never_published(batch, changes):
    snapshot = {"valid_from": "2025-11-02T04:00:00+00:00", "valid_until": "2025-11-03T04:00:00+00:00",
                "scopes": batch.inventory["scopes"]}
    source = _json(batch.tmp / "invalid.json", snapshot | changes)
    with pytest.raises(ValueError):
        stage.stage_registry(batch.root, source)
    assert not (batch.root / "registry").exists()


@pytest.mark.parametrize("change", ["wrong-cycle", "no-registry", "duplicate-scope", "invalid-target", "duplicate-key"])
def test_refuses_an_invalid_first_plan(batch, change):
    if change == "duplicate-key":
        batch.plan.write_text(batch.plan.read_text().replace('"cycle_id":', '"cycle_id": "other", "cycle_id":'))
    else:
        if change == "wrong-cycle":
            batch.inventory["cycle_id"] = "another"
        elif change == "no-registry":
            batch.inventory["registry_snapshot_id"] = ""
        elif change == "duplicate-scope":
            batch.inventory["scopes"] *= 2
        else:
            batch.inventory["scopes"][0]["coaches"] = []
        _json(batch.plan, batch.inventory)
    with pytest.raises((ValueError, RuntimeError)):
        _stage(batch)
    assert not (batch.directory / "plan.json").exists()


def test_captures_require_a_preexisting_plan_and_an_airflow_start(batch):
    with pytest.raises(FileNotFoundError):
        _capture(batch, batch.response().envelope_id)
    _stage(batch)
    journal = _json(batch.tmp / "ok.json", batch.response().envelope_id)
    with pytest.raises(ValueError, match="timezone"):
        stage.stage_captures(batch.root, CYCLE, [journal], store=batch.store,
                             started_at=START.replace(tzinfo=None), traffic=batch.traffic)
    assert not (batch.directory / "captures.json").exists()


@pytest.mark.parametrize("bad", [
    "transport", "http403", "http502", "challenge", "unknown-body", "foreign-cycle", "foreign-scope",
    "outside-start", "missing-original", "broken-body", "tampered-record", "wrapper-error", "missing-attempts",
    "wrong-count", "missing-scope", "missing-target", "repeat", "empty",
])
def test_invalid_inputs_never_seal_even_when_a_success_is_present(batch, bad):
    ok = batch.response()
    entries = [asdict(ok)]
    if bad == "missing-scope":
        batch.inventory["scopes"].append(dict(batch.inventory["scopes"][0], scope_id="two"))
    if bad == "missing-target":
        batch.inventory["scopes"][0]["coaches"].append("123456:other-coach")
    _json(batch.plan, batch.inventory)
    _stage(batch)
    if bad == "transport":
        rejected = batch.store.store_transport_error(url=batch.url, fetched_at=AT, cycle_id=CYCLE, scope_id="one",
            endpoint="coach_profile", attempt=2, error_kind="connection", error_type="ConnectionError")
        entries.append(rejected.envelope_id)
    elif bad in {"http403", "http502"}:
        entries.append(batch.response(attempt=2, status_code=int(bad[4:])).envelope_id)
    elif bad == "challenge":
        entries = [batch.response(body=b"<html><title>Access denied</title></html>").envelope_id]
    elif bad == "unknown-body":
        entries = [batch.response(body=b"<html>missing coach</html>").envelope_id]
    elif bad == "foreign-cycle":
        entries.append(batch.response(cycle_id="another").envelope_id)
    elif bad == "foreign-scope":
        entries.append(batch.response(scope_id="extra").envelope_id)
    elif bad == "outside-start":
        entries = [batch.response(fetched_at="2025-11-02T03:59:59Z").envelope_id]
    elif bad == "missing-original":
        entries.append("f" * 64)
    elif bad == "broken-body":
        blob = Path(batch.store.root) / batch.store.blob_key(ok.raw_body_hash)
        blob.write_bytes(b"corrupt")
    elif bad == "tampered-record":
        entries[0]["attempt"] = True  # JSON true is not the integer 1 in the raw receipt.
    elif bad == "wrapper-error":
        entries = {"raw_attempts": entries, "errors": ["failed after the last response"]}
    elif bad == "missing-attempts":
        entries = {"raw_attempts": [], "network_fetches": 1}
    elif bad == "wrong-count":
        entries = {"raw_attempts": entries, "network_fetches": 2}
    elif bad == "repeat":
        entries.append(ok.envelope_id)
    elif bad == "empty":
        entries = []
    if isinstance(entries, list) and len(entries) > 1:
        _json(batch.traffic, {"request_attempts": 2})
    with pytest.raises((ValueError, RuntimeError, RawStoreError)):
        _capture(batch, entries)
    assert not (batch.directory / "captures.json").exists()
    assert (batch.directory / "plan.json").read_bytes() == _canonical(batch.inventory)


def test_successful_subset_cannot_hide_a_denial_in_the_same_cycle(batch):
    _stage(batch)
    ok = batch.response()
    batch.response(status_code=403, attempt=2)
    _json(batch.traffic, {"cycle_id": CYCLE, "request_attempts": 2})
    with pytest.raises(ValueError, match="traffic receipt"):
        _capture(batch, ok.envelope_id)
    assert not (batch.directory / "captures.json").exists()


def test_missing_predecessor_rejected_even_with_undercounted_receipt(batch):
    _stage(batch)
    ok = batch.response(attempt=2)
    with pytest.raises(ValueError, match="ordinal"):
        _capture(batch, ok.envelope_id)
    assert not (batch.directory / "captures.json").exists()


@pytest.mark.parametrize("traffic", [
    {}, [], {"request_attempts": 0}, {"request_attempts": True}, {"request_attempts": 1.0},
    {"request_attempts": "1"}, {"request_attempts": 1, "cycle_id": "other"},
    {"request_attempts": 1, "run_id": "other"}, {"request_attempts": -1},
])
def test_malformed_or_foreign_independent_traffic_receipt_is_rejected(batch, traffic):
    _stage(batch)
    _json(batch.traffic, traffic)
    with pytest.raises(ValueError, match="[Tt]raffic"):
        _capture(batch, batch.response().envelope_id)
    assert not (batch.directory / "captures.json").exists()


def test_journal_cannot_double_as_its_independent_traffic_receipt(batch):
    _stage(batch)
    journal = _json(batch.tmp / "journal.json", {"request_attempts": 1, "raw_attempts": [batch.response().envelope_id]})
    other_name = batch.tmp / "other-name.json"
    os.link(journal, other_name)
    with pytest.raises(ValueError, match="separate"):
        stage.stage_captures(batch.root, CYCLE, [journal], store=batch.store, started_at=START, traffic=other_name)
    assert not (batch.directory / "captures.json").exists()


@pytest.mark.parametrize("action", ["plan", "captures"])
def test_two_publishers_elect_one_winner_and_cannot_change_its_file(batch, action):
    if action == "plan":
        other = _json(batch.tmp / "other.json", dict(batch.inventory, registry_snapshot_id="registry-v2"))
        inputs = [batch.plan, other]
        def publish(source):
            return stage.stage_plan(batch.root, CYCLE, source)
    else:
        _stage(batch)
        inputs = [_json(batch.tmp / f"journal-{i}.json", batch.response(fetched_at=f"2025-11-02T04:02:0{i}Z").envelope_id)
                  for i in (1, 2)]
        def publish(source):
            return stage.stage_captures(batch.root, CYCLE, [source], store=batch.store, started_at=START, traffic=batch.traffic)
    barrier = Barrier(2)
    def contender(source):
        barrier.wait(timeout=10)
        try:
            publish(source)
        except RuntimeError as error:
            assert "already published" in str(error)
            return False
        return True
    with ThreadPoolExecutor(max_workers=2) as pool:
        winners = list(pool.map(contender, inputs))
    assert sorted(winners) == [False, True]
    source = inputs[winners.index(True)]
    before = (batch.directory / f"{action}.json").stat()
    output = publish(source)
    assert output.stat().st_ino == before.st_ino and output.stat().st_mtime_ns == before.st_mtime_ns
    if action == "plan":
        assert output.read_bytes() == _canonical(json.loads(source.read_bytes()))
    else:
        assert json.loads(output.read_bytes())["envelopes_by_scope"] == {"one": [json.loads(source.read_bytes())]}
