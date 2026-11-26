"""Exercise scheduled publication against recorded source bytes and a local warehouse."""

import gzip
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import urlsplit

from pyarrow import fs
import pytest

from dags.scripts import run_transfermarkt_bronze_cycle as cycle
from scrapers.transfermarkt import native_writer
from scrapers.transfermarkt.raw_store import RawResponseStore
from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB, CoreWriter


pytestmark = pytest.mark.unit
FIXTURES = Path("tests/fixtures/transfermarkt/stage_a_rewrite")
POLICY = Path("dags/configs/transfermarkt/standing_approval_policy.json")
TABLE = "iceberg.bronze.transfermarkt_coach_profiles"
TABLES = {TABLE, native_writer.CORE_RECEIPTS_TABLE, native_writer.CORE_PAGES_TABLE}


def _write_json(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


def _plan_bytes(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _publish_test_inventory(root, snapshot, monkeypatch):
    data = json.dumps(snapshot).encode()
    digest = hashlib.sha256(data).hexdigest()
    directory = root / "registry"
    directory.mkdir(exist_ok=True)
    (directory / (digest + ".json")).write_bytes(data)
    monkeypatch.setenv("TM_CORE_INVENTORY_SHA256", digest)
    return digest


def _save_evidence(directory, index):
    # This fixture simulates the two separate outputs of a bounded collector.
    ids = []
    for value in index["envelopes_by_scope"].values():
        value.sort()
        ids.extend(value)
    journal = _plan_bytes(ids)
    journal_digest = hashlib.sha256(journal).hexdigest()
    evidence = directory / "evidence"
    evidence.mkdir(exist_ok=True)
    (evidence / (journal_digest + ".json")).write_bytes(journal)
    traffic = _plan_bytes({"cycle_id": index["cycle_id"], "request_attempts": len(ids)})
    (directory / "traffic.json").write_bytes(traffic)
    index.update(attempt_journals=[journal_digest], traffic_sha256=hashlib.sha256(traffic).hexdigest())
    _write_json(directory / "captures.json", index)


@pytest.fixture
def archived(tmp_path, monkeypatch):
    store = RawResponseStore(fs.LocalFileSystem(), str(tmp_path / "raw"),
                             uri_prefix="s3://warehouse/raw/transfermarkt")
    monkeypatch.setattr(RawResponseStore, "from_env", staticmethod(lambda: store))
    # Two coach profiles were actually collected together in this archived cycle.
    cycle_id = "tm-child-c389157dbad1a6a34c1ba68a"
    profiles, targets, envelope_ids = {}, [], []
    for source in json.loads((FIXTURES / "manifest.json").read_text())["fixtures"]:
        if source["endpoint"] != "coach_profile":
            continue
        record = json.loads((FIXTURES / source["capture_file"]).read_text())
        if record["cycle_id"] != cycle_id:
            continue
        body = gzip.decompress((FIXTURES / source["blob_file"]).read_bytes())
        original = store.store_attempt(body=body, **{key: record[key] for key in (
            "url", "status_code", "headers", "fetched_at", "cycle_id", "scope_id", "endpoint", "attempt",
        )})
        assert (original.capture_id, original.content_hash) == (record["capture_id"], record["content_hash"])
        # The historical batch had other attempts. The complete two-call test
        # cycle uses its own identity/ordinals and preserves the source page bytes.
        original = store.store_attempt(original.url, body, 200, original.headers, original.fetched_at,
                                       "scheduled__two_coaches", original.scope_id, original.endpoint,
                                       len(profiles) + 1)
        slug, _, _, coach = urlsplit(original.url).path.strip("/").split("/")
        targets.append(f"{coach}:{slug}")
        profiles[coach] = original
        envelope_ids.append(store.store_response_envelope(original).envelope_id)
    assert len(profiles) == 2 and len({capture.scope_id for capture in profiles.values()}) == 1
    original = min(profiles.values(), key=lambda capture: capture.fetched_at)
    scope = {"league": "JAM1", "season": "2627", "edition_id": "2026",
             "scope_id": original.scope_id, "coaches": targets}

    directory = tmp_path / "cycles" / hashlib.sha256(original.cycle_id.encode()).hexdigest()
    directory.mkdir(parents=True)
    started_at = datetime.fromisoformat(original.fetched_at) - timedelta(seconds=1)
    snapshot = {"valid_from": (started_at - timedelta(days=1)).isoformat(),
                "valid_until": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(), "scopes": [scope]}
    digest = _publish_test_inventory(tmp_path, snapshot, monkeypatch)
    plan = {"cycle_id": original.cycle_id, "registry_snapshot_id": digest, "scopes": [scope]}
    plan_file, index_file = directory / "plan.json", directory / "captures.json"
    plan_file.write_bytes(_plan_bytes(plan))
    index = {"cycle_id": original.cycle_id, "plan_sha256": hashlib.sha256(plan_file.read_bytes()).hexdigest(),
             "envelopes_by_scope": {original.scope_id: envelope_ids}}
    _save_evidence(directory, index)

    policy = json.loads(POLICY.read_text())
    now = datetime.now(timezone.utc)
    policy.update(approved_at=(now - timedelta(days=1)).isoformat(),
                  expires_at=(now + timedelta(days=1)).isoformat(), allowed_write_tables=sorted(TABLES))
    policy_file = tmp_path / "test-policy.json"
    _write_json(policy_file, policy)
    monkeypatch.setenv("TM_STANDING_POLICY_ENABLED", "true")
    monkeypatch.setenv("TM_WRITER_LOCK_PATH", str(tmp_path / "writer.lock"))
    monkeypatch.setenv("TM_CORE_WRITE_JOURNAL_DIR", str(tmp_path / "journal"))
    monkeypatch.delenv("TM_WRITER_LOCK_SHARED_FILE_ID", raising=False)
    db = CoreDB()
    writer = CoreWriter(db)
    write_options = Mock(return_value={"connection_factory": lambda: db, "writer": writer})
    monkeypatch.setattr(cycle.cli, "_write_options", write_options)
    return SimpleNamespace(root=tmp_path, store=store, original=original, profiles=profiles, scope=scope,
                           plan=plan, plan_file=plan_file, index=index, index_file=index_file,
                           policy=policy, policy_file=policy_file, db=db, write_options=write_options,
                           writer=writer, started_at=started_at, snapshot=snapshot, monkeypatch=monkeypatch)


def _publish(archive):
    return cycle.publish_cycle(archive.root, archive.policy_file, cycle_id=archive.original.cycle_id,
                               started_at=archive.started_at)


def _save_plan_and_index(archive):
    archive.snapshot["scopes"] = archive.plan["scopes"]
    archive.plan["registry_snapshot_id"] = _publish_test_inventory(archive.root, archive.snapshot, archive.monkeypatch)
    archive.plan_file.write_bytes(_plan_bytes(archive.plan))
    archive.index["plan_sha256"] = hashlib.sha256(archive.plan_file.read_bytes()).hexdigest()
    _save_evidence(archive.index_file.parent, archive.index)


def _assert_rejected(archive, message):
    with pytest.raises(ValueError, match=message):
        _publish(archive)
    archive.write_options.assert_not_called()
    assert archive.writer.writes == []


def test_original_cycle_publishes_and_reruns_without_duplicate_rows_or_receipts(archived):
    expected = {"cycle_id": archived.original.cycle_id, "registry_snapshot_id": archived.plan["registry_snapshot_id"],
                "scopes": 1, "rows": 2, "receipts": 2, "tables": sorted(TABLES)}
    assert _publish(archived) == expected
    bronze = archived.db.sql.execute(
        "SELECT coach_id, raw_capture_id, source_body_hash, captured_at, scope_id, cycle_id, universe_version "
        "FROM transfermarkt_coach_profiles"
    ).fetchall()
    assert {row[0] for row in bronze} == set(archived.profiles)
    for coach_id, capture, digest, stamp, scope, cycle_id, universe in bronze:
        original = archived.profiles[coach_id]
        assert (capture, digest, scope, cycle_id, universe) == (
            original.capture_id, original.content_hash, original.scope_id, original.cycle_id,
            archived.plan["registry_snapshot_id"])
        assert datetime.fromisoformat(str(stamp)) == datetime.fromisoformat(original.fetched_at)
    stored_receipts = archived.db.sql.execute(
        "SELECT source_capture_id, source_body_hash, row_count FROM transfermarkt_core_receipts_v1"
    ).fetchall()
    assert sorted(stored_receipts) == sorted((capture.capture_id, capture.content_hash, 1)
                                             for capture in archived.profiles.values())
    rows = archived.db.sql.execute("SELECT * FROM transfermarkt_coach_profiles").fetchall()
    receipts = archived.db.sql.execute("SELECT * FROM transfermarkt_core_receipts_v1").fetchall()
    writes = list(archived.writer.writes)
    assert _publish(archived) == expected
    assert archived.db.sql.execute("SELECT * FROM transfermarkt_core_receipts_v1").fetchall() == receipts
    assert archived.db.sql.execute("SELECT * FROM transfermarkt_coach_profiles").fetchall() == rows
    assert archived.writer.writes == writes


@pytest.mark.parametrize("change", ["plan-cycle", "index-cycle", "plan-content"])
def test_mismatched_cycle_or_changed_plan_fails_before_writer(archived, change):
    if change == "plan-cycle":
        archived.plan["cycle_id"] = "different-cycle"
        _write_json(archived.plan_file, archived.plan)
        archived.index["plan_sha256"] = hashlib.sha256(archived.plan_file.read_bytes()).hexdigest()
    elif change == "index-cycle":
        archived.index["cycle_id"] = "different-cycle"
    else:
        archived.plan["registry_snapshot_id"] = "another-registry"
        _write_json(archived.plan_file, archived.plan)
    _write_json(archived.index_file, archived.index)
    with pytest.raises((ValueError, RuntimeError), match="original plan|approved Transfermarkt Bronze inventory"):
        _publish(archived)
    archived.write_options.assert_not_called()


def test_second_planned_scope_has_no_source_responses(archived):
    archived.plan["scopes"].append(dict(archived.scope, scope_id="missing"))
    _save_plan_and_index(archived)
    _assert_rejected(archived, "Missing source responses")


def test_second_scope_403_prevents_first_valid_scope_from_writing(archived):
    original = archived.original
    second_scope = dict(archived.scope, scope_id="second-scope")
    denied = archived.store.store_attempt(original.url, b"Forbidden", 403, {}, original.fetched_at,
                                         original.cycle_id, second_scope["scope_id"], original.endpoint, 1)
    envelope = archived.store.store_response_envelope(denied)
    archived.plan["scopes"].append(second_scope)
    archived.index["envelopes_by_scope"][second_scope["scope_id"]] = [envelope.envelope_id]
    _save_plan_and_index(archived)
    _assert_rejected(archived, "403")


def test_unplanned_scope_is_rejected(archived):
    envelopes = archived.index["envelopes_by_scope"]
    envelopes["extra"] = envelopes[archived.original.scope_id]
    _write_json(archived.index_file, archived.index)
    _assert_rejected(archived, "Captured and planned scopes differ")


def test_missing_target_within_a_valid_scope_prevents_partial_publication(archived):
    archived.scope["coaches"].append("123456:another-coach")
    _save_plan_and_index(archived)
    _assert_rejected(archived, "missing or repeated")


@pytest.mark.parametrize("foreign", ["scope_id", "cycle_id"])
def test_foreign_original_fails_even_if_plan_and_capture_index_agree(archived, foreign):
    original = archived.original
    body, _ = archived.store.load_capture(original.capture_id)
    context = {"cycle_id": original.cycle_id, "scope_id": original.scope_id, foreign: "other"}
    other = archived.store.store_attempt(original.url, body, 200, original.headers, original.fetched_at,
                                        endpoint=original.endpoint, attempt=1, **context)
    envelope = archived.store.store_response_envelope(other)
    archived.index["envelopes_by_scope"][original.scope_id] = [envelope.envelope_id]
    _save_evidence(archived.index_file.parent, archived.index)
    _assert_rejected(archived, "another cycle or an unplanned scope")


def test_page_fetched_before_dag_start_is_rejected(archived):
    archived.started_at = datetime.fromisoformat(archived.original.fetched_at) + timedelta(microseconds=1)
    _assert_rejected(archived, "outside the current Airflow run")


def test_missing_bronze_table_grant_is_rejected_before_writer(archived):
    archived.policy["allowed_write_tables"].remove(TABLE)
    _write_json(archived.policy_file, archived.policy)
    _assert_rejected(archived, "does not cover")


def test_repeated_index_key_cannot_override_the_expected_envelopes(archived):
    index = archived.index_file.read_text().replace(
        '"envelopes_by_scope":', '"envelopes_by_scope": {}, "envelopes_by_scope":')
    archived.index_file.write_text(index)
    _assert_rejected(archived, "Duplicate JSON key")


@pytest.mark.parametrize("change", ["missing-pin", "digest-mismatch", "expired", "not-yet-current", "missing-scope"])
def test_publisher_requires_the_current_independently_pinned_inventory(archived, change):
    digest = archived.plan["registry_snapshot_id"]
    if change == "missing-pin":
        archived.monkeypatch.delenv("TM_CORE_INVENTORY_SHA256")
    elif change == "digest-mismatch":
        (archived.root / "registry" / (digest + ".json")).write_bytes(b"{}")
    elif change == "missing-scope":
        archived.plan["scopes"] = [dict(archived.scope, coaches=archived.scope["coaches"][:1])]
        archived.plan_file.write_bytes(_plan_bytes(archived.plan))
        archived.index["plan_sha256"] = hashlib.sha256(archived.plan_file.read_bytes()).hexdigest()
        _write_json(archived.index_file, archived.index)
    else:
        field = "valid_until" if change == "expired" else "valid_from"
        stamp = archived.started_at - timedelta(days=1) if change == "expired" else datetime.now(timezone.utc)
        archived.snapshot[field] = stamp.isoformat()
        _publish_test_inventory(archived.root, archived.snapshot, archived.monkeypatch)
    with pytest.raises((ValueError, RuntimeError), match="inventory|Inventory"):
        _publish(archived)
    archived.write_options.assert_not_called()
    assert archived.writer.writes == []


def test_two_scopes_claiming_the_same_observations_fail_before_any_write(archived):
    second = dict(archived.scope, scope_id="another-coach-scope")
    archived.plan["scopes"].append(second)
    envelope_ids = []
    for ordinal, original in enumerate(archived.profiles.values(), 1):
        body, _ = archived.store.load_capture(original.capture_id)
        capture = archived.store.store_attempt(original.url, body, original.status_code, original.headers,
                                              original.fetched_at, original.cycle_id, second["scope_id"],
                                              original.endpoint, ordinal)
        envelope_ids.append(archived.store.store_response_envelope(capture).envelope_id)
    archived.index["envelopes_by_scope"][second["scope_id"]] = sorted(envelope_ids)
    _save_plan_and_index(archived)
    _save_evidence(archived.index_file.parent, archived.index)

    with pytest.raises((ValueError, RuntimeError), match="natural key") as failure:
        _publish(archived)
    assert archived.db.sql.execute(
        "SELECT name FROM sqlite_master WHERE name IN ('transfermarkt_coach_profiles', "
        "'transfermarkt_core_receipts_v1')"
    ).fetchall() == []
    archived.write_options.assert_not_called()
    assert archived.writer.writes == []
    assert str(failure.value) == "duplicate core natural key across plans"


@pytest.mark.parametrize("damage", ["omitted-403", "other-cycle-meter", "unbound-meter", "missing-journal",
                                    "wrong-journal-digest", "wrong-meter-digest", "index-only"])
def test_writer_independently_verifies_bounded_input_evidence(archived, damage):
    directory = archived.index_file.parent
    index = archived.index
    meter = {"cycle_id": archived.original.cycle_id, "request_attempts": 2}
    error = "receipt|journal|index"
    if damage == "omitted-403":
        # Genuine valid originals still fill every planned target. A third
        # original was refused. The immutable journal and meter recorded it.
        original = archived.original
        denied = archived.store.store_attempt(original.url, b"Forbidden", 403, {}, original.fetched_at,
            original.cycle_id, original.scope_id, original.endpoint, 3)
        denied_id = archived.store.store_response_envelope(denied).envelope_id
        evidence = directory / "evidence"
        ids = json.loads((evidence / (index["attempt_journals"][0] + ".json")).read_bytes()) + [denied_id]
        data = _plan_bytes(ids)
        key = hashlib.sha256(data).hexdigest()
        (evidence / (key + ".json")).write_bytes(data)
        index["attempt_journals"] = [key]
        meter["request_attempts"] = 3
    elif damage == "other-cycle-meter":
        meter["cycle_id"] = "previous"
    elif damage == "unbound-meter":
        del meter["cycle_id"]
    elif damage == "missing-journal":
        index["attempt_journals"] = []
    elif damage == "wrong-journal-digest":
        (directory / "evidence" / (index["attempt_journals"][0] + ".json")).write_bytes(b"[]")
    elif damage == "index-only":
        index.pop("attempt_journals")
    data = _plan_bytes(meter)
    (directory / "traffic.json").write_bytes(data)
    index["traffic_sha256"] = "0" * 64 if damage == "wrong-meter-digest" else hashlib.sha256(data).hexdigest()
    _write_json(archived.index_file, index)
    _assert_rejected(archived, error)
