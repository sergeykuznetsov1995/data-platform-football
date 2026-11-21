"""Synthetic-only tests: importing this module never contacts Docker or Trino."""

import argparse
import csv
import importlib.util
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[3] / "scripts/research/audit_fotmob_completion.py"
SPEC = importlib.util.spec_from_file_location("audit_fotmob_completion", SCRIPT)
audit = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(audit)

CAPTURED_AT = "2026-11-16T00:00:00Z"


def manifest(mid=1, batch="b1", status="success", parser="fotmob-v2", hour=0, **extra):
    return {"competition_id": 1, "source_season_key": "2008 Final Stage", "entity_id": str(mid),
            "target_key": f"match:{mid}", "batch_id": batch, "parser_version": parser,
            "status": status, "fetch_outcome": "ok", "error_code": None,
            "fetched_at": f"2026-11-15T{hour:02d}:00:00Z", "completed_at": f"2026-11-15T{hour:02d}:00:00Z",
            "expected_counts_json": '{"match_payloads":1}', "actual_counts_json": '{}',
            "capabilities_json": '{}', "run_id": batch, **extra}


def match(mid=1, batch="calendar", **extra):
    return {"competition_id": 1, "source_season_key": "2008 Final Stage", "match_id": mid,
            "parser_version": "fotmob-v2", "_target_batch_id": batch,
            "_observed_at": "2026-11-15T00:00:00Z", "_ingested_at": "2026-11-15T00:01:00Z",
            "finished": True, "cancelled": False, "postponed": False, "awarded": False,
            "utc_time": "2026-11-14T00:00:00Z", **extra}


def payload(mid=1, batch="b1", **extra):
    row = {**match(mid, batch), "available_sections": list(audit.SECTIONS), "payload_json_valid": True}
    row.update({prefix + section: True for prefix in ("raw_", "typed_") for section in audit.SECTIONS})
    row.update(extra)
    return row


def unavailable(batch, hour, run=None):
    return manifest(batch=batch, status="not_available", hour=hour,
                    error_code="source_data_not_found", run_id=run or batch)


def put_csv(path, fields, rows):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def refresh_content_hash(directory, path):
    hashes = audit.load(directory / "content-hashes.json")
    hashes[path.name] = audit.file_sha(path)
    audit.dump(directory / "content-hashes.json", hashes)


def inputs(tmp_path):
    registry = tmp_path / "registry.csv"
    historical = tmp_path / "historical.csv"
    rows = [{"competition_id": cid, "catalog_name": f"Cup {cid}", "decision": "include",
             "required_season_keys": "2008 Final Stage"} for cid in range(1, 19)]
    put_csv(registry, tuple(rows[0]), rows)
    old = [{"competition_id": cid, "source_season_key": "2008 Final Stage",
            "old_gap": 100 if cid < 18 else 62} for cid in range(1, 19)]
    put_csv(historical, tuple(old[0]), old)
    return registry, historical


def test_registry_exact_labels_and_pre_2010(tmp_path):
    path = tmp_path / "registry.csv"
    put_csv(path, ("competition_id", "catalog_name", "decision", "required_season_keys"), [
        {"competition_id": 42, "catalog_name": "EURO", "decision": "include",
         "required_season_keys": "2008|2006 Final Stage| 2004 "},
        {"competition_id": 43, "catalog_name": "Ignore", "decision": "exclude", "required_season_keys": ""}])
    result = audit.load_registry(path, 3)
    assert set(result) == {(42, "2008"), (42, "2006 Final Stage"), (42, " 2004 ")}
    with pytest.raises(audit.AuditError, match="required scopes"):
        audit.load_registry(path, 5540)


def test_historical_validates_exact_18_and_preserves_bytes(tmp_path):
    _, path = inputs(tmp_path)
    original = path.read_bytes()
    rows = audit.load_historical(path)
    assert len(rows) == 18 and sum(row["old_gap"] for row in rows) == 1762
    assert original == path.read_bytes()
    path.write_bytes(original.replace(b"18,", b"17,"))
    with pytest.raises(audit.AuditError):
        audit.load_historical(path)


@pytest.mark.parametrize("count", [0, 17, 19])
def test_historical_wrong_row_count(tmp_path, count):
    path = tmp_path / "old.csv"
    put_csv(path, ("competition_id", "source_season_key", "old_gap"), [
        {"competition_id": i + 1, "source_season_key": "2008", "old_gap": 1} for i in range(count)])
    with pytest.raises(audit.AuditError, match="18"):
        audit.load_historical(path)


def test_v2_priority_source_observation_and_fallback():
    old = manifest(batch="v2-old", hour=1)
    new_v1 = manifest(batch="v1-new", parser="fotmob-v1", hour=20)
    observed = manifest(batch="v2-source", hour=0,
                        capabilities_json='{"source_observed_at":"2026-11-15T22:00:00Z"}')
    assert audit.choose_global([new_v1, old, observed])[1]["batch_id"] == "v2-source"
    observed["capabilities_json"] = '{"source_observed_at":"not-a-timestamp"}'
    assert audit.choose_global([new_v1, old, observed])[1]["batch_id"] == "v2-old"
    observed["capabilities_json"] = '{"capabilities_json":{"source_observed_at":"2027-01-01T00:00:00Z"}}'
    assert audit.choose_global([old, observed])[1]["batch_id"] == "v2-old"


def test_v1_not_available_ignored_and_v2_tombstone():
    old = manifest(batch="old")
    tombstone = manifest(batch="gone", hour=2, status="not_available")
    key = audit.scope_key(old)
    presence = {(*key, "old")}
    winners, _ = audit.choose_calendar([old, {**tombstone, "parser_version": "fotmob-v1"}], presence)
    assert winners[key]["batch_id"] == "old"
    winners, _ = audit.choose_calendar([old, tombstone], presence)
    assert winners[key]["status"] == "not_available"
    assert not audit.eligible(manifest(parser="unrecognized-v9"))


def test_empty_success_skipped_and_presence_is_scope_specific():
    old, new = manifest(batch="old"), manifest(batch="new", hour=1)
    key = audit.scope_key(old)
    selected, anomalies = audit.choose_calendar([old, new], {(*key, "old"), (999, key[1], "new")})
    assert selected[key]["batch_id"] == "old"
    assert anomalies[0]["reason"] == "empty_calendar_success_skipped"


def test_calendar_natural_dedup_no_cross_batch_merge():
    selected = manifest(batch="new", hour=1)
    old_rows = [match(1, "old"), match(2, "old")]
    new_rows = [match(1, "new"), match(1, "new", _observed_at="2026-11-15T01:00:00Z", cancelled=True)]

    class Fake:
        def run(self, sql, step, **kwargs):
            return {"league_manifest": [manifest(batch="old"), selected],
                    "match_batch_presence": [{"competition_id": 1, "source_season_key": selected["source_season_key"],
                                               "_target_batch_id": value} for value in ("old", "new")],
                    "current_matches": old_rows + new_rows}[step]

    _, current, _ = audit.read_scope_chunk(Fake(), snapshots(), [selected], "0")
    assert set(current) == {(1, "2008 Final Stage", 1)}
    assert current[(1, "2008 Final Stage", 1)]["cancelled"]


def test_equal_physical_order_conflict_fails():
    with pytest.raises(audit.AuditError, match="ambiguous"):
        audit.deduplicate([match(), match(cancelled=True)])


def test_global_winner_shared_id_and_scoped_membership():
    calendar = audit.deduplicate([match(), match(competition_id=2)])
    winners = audit.choose_global([manifest(batch="old"), manifest(batch="new", hour=1, competition_id=2)])
    rows = [payload(batch="old"), payload(batch="new", competition_id=2),
            payload(batch="new", competition_id=3), payload(2, "new")]
    current = audit.current_payloads(rows, calendar, winners)
    assert set(current) == {(2, "2008 Final Stage", 1)}
    assert winners[1]["physical_batch_present"]
    scoped = [audit.scoped_id(match()), audit.scoped_id(match(competition_id=2))]
    assert len(set(scoped)) == 2
    assert audit.id_set_hash(scoped) != audit.id_set_hash(["1"])
    assert audit.id_set_hash(["1", "1"]) == audit.id_set_hash(["1"])


def test_scope_id_encoding_is_unambiguous():
    assert audit.scoped_id(match(source_season_key='x|y"z')) == '[1,"x|y\\"z",1]'
    assert audit.id_set_hash([]) == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert audit.id_set_hash(["z", "a"]) == audit.sha(b"a\0z")


@pytest.mark.parametrize("actual", ['{}', '{"match_payloads":0}', '{"match_payloads":1}'])
def test_expected_one_missing_writer_failure_even_empty_actual_counts(actual):
    assert audit.classify_missing(match(), manifest(actual_counts_json=actual), [])[0] == "writer_failed"


def test_raw_uri_alone_not_writer_evidence():
    winner = manifest(expected_counts_json='{}', actual_counts_json='{}', raw_uri="s3://synthetic/example")
    assert audit.classify_missing(match(), winner, [])[0] == "unknown"
    winner["physical_batch_present"] = True
    assert audit.classify_missing(match(), winner, [])[0] == "writer_failed"


@pytest.mark.parametrize("history,expected", [
    ([unavailable("a", 0)], "unknown"),
    ([unavailable("a", 0), unavailable("b", 1)], "unknown"),
    ([unavailable("a", 0), unavailable("b", 2)], "source_unavailable"),
    ([unavailable("a", 0), unavailable("b", 0), unavailable("c", 0)], "source_unavailable"),
    ([unavailable("a", 0, "same"), unavailable("b", 3, "same")], "unknown"),
])
def test_repeated_source_absence(history, expected):
    assert audit.classify_missing(match(), None, history)[0] == expected


def test_later_success_resets_unavailable_run_evidence():
    history = [unavailable("a", 0), unavailable("b", 3), manifest(batch="success", hour=4), unavailable("c", 5)]
    assert audit.classify_missing(match(), history[-1], history)[0] == "unknown"


def test_terminal_transport_vs_schema_parser_failure():
    terminal = manifest(status="terminal_failure", error_code="http_503")
    assert audit.classify_missing(match(), None, [terminal])[0] == "fetch_failed"
    for row in [manifest(status="schema_drift"), manifest(status="terminal_failure", error_code="parser_invalid")]:
        assert audit.classify_missing(match(), None, [row])[0] == "parse_failed"


@pytest.mark.parametrize("flag,reason", [("cancelled", "cancelled"), ("postponed", "postponed"), ("future", "future")])
def test_explicit_not_applicable(flag, reason):
    assert audit.classify_missing(match(**{flag: True}), None, []) == ("not_applicable", reason)


def test_awarded_unfinished_not_collected_or_not_applicable():
    assert audit.classify_missing(match(finished=False, awarded=True), None, []) == ("unknown", "awarded_unfinished")


@pytest.mark.parametrize("value,expected", [(None, False), ({}, False), ([], False), (False, True), (0, True),
                                           ("", True), ([0], True), ({"a": None}, True)])
def test_service_nonempty(value, expected):
    assert audit.nonempty(value) is expected


@pytest.mark.parametrize("section", audit.SECTIONS)
def test_each_section_raw_typed_mask_matrix(section):
    row = payload()
    assert all(item["category"] == "confirmed_content" for item in audit.section_results(row, True))
    row["typed_" + section] = False
    item = next(item for item in audit.section_results(row, True) if item["section"] == section)
    assert item["category"] == "writer_failed"
    row["typed_" + section], row["raw_" + section] = True, False
    item = next(item for item in audit.section_results(row, True) if item["section"] == section)
    assert item["category"] == "unknown"
    row["typed_" + section] = False
    row["available_sections"].remove(section)
    item = next(item for item in audit.section_results(row, True) if item["section"] == section)
    assert item["category"] == "source_unavailable" and item["reason"] == "stored_source_omission"


def test_malformed_payload_and_mask_are_unknown():
    assert all(row["category"] == "unknown" for row in audit.section_results(payload(payload_json_valid=False), True))
    assert all(row["category"] == "unknown" for row in audit.section_results(payload(available_sections="bad"), True))
    row = payload(available_sections=[])
    assert all(item["reason"] == "available_mask_disagrees" for item in audit.section_results(row, True))


def test_finished_matchfacts_omission_requires_schema_evidence_for_parse_failure():
    row = payload(raw_matchFacts=False, typed_matchFacts=False,
                  available_sections=[name for name in audit.SECTIONS if name != "matchFacts"])
    assert audit.section_results(row, True)[0]["category"] == "source_unavailable"
    assert audit.section_results(row, True, [manifest(status="schema_drift")])[0]["category"] == "parse_failed"


@pytest.mark.parametrize("section", audit.SECTIONS)
def test_2302_false_sentinel_with_stale_mask_is_definite_source_omission(section):
    # Pre-#2302 batches stored the source's JSON false sentinel while the
    # mask still claimed the section available (fixed in e46256b5). Raw and
    # typed evidence both prove absence, so the recount must return a
    # definite source_unavailable instead of unknown:available_mask_disagrees.
    row = payload(**{"typed_" + section: False, "raw_" + section: False})
    item = next(item for item in audit.section_results(row, True) if item["section"] == section)
    assert item["category"] == "source_unavailable"
    assert item["reason"] == "stored_source_omission_mask_stale"
    assert item["available"] is True


def test_2302_momentum_column_unparseable_stays_unknown_not_guessable():
    row = payload(typed_momentum=None)
    item = next(item for item in audit.section_results(row, True) if item["section"] == "momentum")
    assert item["category"] == "unknown" and item["reason"] == "malformed_or_unresolved_json"


def test_2302_payload_sql_classifies_false_sentinel_as_absent():
    sql = (SCRIPT.parents[0] / "fotmob_completion_audit/current_payloads.sql").read_text(encoding="utf-8")
    # The false sentinel never counts as content, in any section, on both the
    # typed and the raw side.
    assert sql.count("IN ('null', 'false', '{{}}', '[]')") == 12
    # momentum_json is BOOLEAN-typed in Bronze and may hold the legacy Python
    # spelling 'False': read it through VARCHAR and normalize the spelling.
    assert "lower(trim(CAST(momentum_json AS VARCHAR))) = 'false'" in sql
    assert "json_parse(momentum_json)" not in sql
    assert "json_parse(CAST(momentum_json AS VARCHAR))" in sql


def test_marker_decodes_families_and_explicit_scopes():
    row = manifest(capabilities_json='{"coverage":{"scope_entities":["matches","match_payloads"]}}')
    assert audit.marker_scopes(row)[1:] == (True, True)
    row["capabilities_json"] = '{"coverage":{"scope_entities":["standings"]}}'
    assert audit.marker_scopes(row) == ([], True, False)
    row["capabilities_json"] = '{"coverage":{"scope_entities":{"matches":[{"competition_id":1,"source_season_key":"2008 Final Stage"}]}}}'
    assert audit.marker_scopes(row)[0] == [{"competition_id": 1, "source_season_key": "2008 Final Stage"}]
    row["capabilities_json"] = '{broken'
    assert audit.marker_scopes(row)[1] is False


def test_primary_unknown_overrides_all_and_other_priority():
    assert audit.primary({"confirmed_content": 99, "unknown": 1}) == "unknown"
    assert audit.primary({"writer_failed": 1, "parse_failed": 2}) == "writer_failed"
    assert audit.primary({"source_unavailable": 1, "confirmed_content": 10}) == "source_unavailable"


def source_gap():
    signature = "synthetic-plan-signature"
    proof = {"match_id": 1, "target_key": "b" * 64, "raw_uri": "s3://synthetic/gap.json",
             "content_hash": "a" * 64, "run_id": "source-observation",
             "observed_at": "2026-11-15T04:00:00Z", "reason": "source_data_not_found"}
    capabilities = {"outcome": "source_gap", "reason": "source_data_not_found",
                    "plan_signature": signature, "scope_entities": ["matches", "match_payloads"],
                    "match_ids": [1, 2], "available_match_ids": [2], "source_gap_matches": [proof]}
    return manifest(status="not_available", hour=5, entity_id=signature,
                    target_key="scope-gap:1", capabilities_json=json.dumps(capabilities))


def test_source_gap_proof_hash_count_and_ids():
    row = source_gap()
    proofs, anomalies = audit.decode_source_gaps([row])
    assert set(proofs) == {(1, "2008 Final Stage", 1)} and not anomalies
    decoded = proofs[(1, "2008 Final Stage", 1)]
    proof = json.loads(row["capabilities_json"])["source_gap_matches"][0]
    assert decoded["evidence"] == proof and decoded["plan_signature"] == row["entity_id"]
    assert decoded["run_id"] == proof["run_id"]
    assert decoded["evidence_sha256"] == audit.sha(audit.canonical({"manifest": row, "proof": proof}).encode())
    assert audit.classify_missing(match(), None, [], decoded)[0] == "source_unavailable"
    assert audit.classify_missing(match(), None, [manifest(hour=6)], decoded)[0] == "unknown"


@pytest.mark.parametrize("mutation", [
    "legacy", "identity", "outcome", "scope", "duplicates", "overlap", "incomplete", "duplicate_proof",
    "target", "raw_uri", "hash", "run", "observed", "reason", "entities", "status", "parser", "batch",
])
def test_source_gap_malformed_or_nonpartitioned_proof_fails_closed(mutation):
    row = source_gap()
    capabilities = json.loads(row["capabilities_json"])
    proof = capabilities["source_gap_matches"][0]
    if mutation == "legacy":
        capabilities = {"scope_source_gap": {"match_ids": [1], "count": 1,
                                             "id_set_hash": audit.id_set_hash(["1"])}}
    elif mutation == "identity":
        row["entity_id"] = "different-plan"
    elif mutation == "outcome":
        capabilities["outcome"] = "success"
    elif mutation == "scope":
        row["source_season_key"] = ""
    elif mutation == "duplicates":
        capabilities["match_ids"] = [1, 1, 2]
    elif mutation == "overlap":
        capabilities["available_match_ids"] = [1, 2]
    elif mutation == "incomplete":
        capabilities["match_ids"].append(3)
    elif mutation == "duplicate_proof":
        capabilities["source_gap_matches"].append(dict(proof))
    elif mutation == "entities":
        capabilities["scope_entities"] = ["match_payloads"]
    elif mutation in {"status", "parser", "batch"}:
        field, value = {"status": ("status", "success"), "parser": ("parser_version", "fotmob-v1"),
                        "batch": ("batch_id", "")}[mutation]
        row[field] = value
    else:
        field, value = {"target": ("target_key", "match:2"), "raw_uri": ("raw_uri", "s3://bucket"),
                        "hash": ("content_hash", "bad"), "run": ("run_id", " "),
                        "observed": ("observed_at", "2026-11-15T06:00:00Z"),
                        "reason": ("reason", "http_503")}[mutation]
        proof[field] = value
    row["capabilities_json"] = json.dumps(capabilities)
    assert audit.decode_source_gaps([row])[0] == {}
    assert audit.decode_source_gaps([row])[1][0]["reason"] == "unresolved_scope_source_gap"


def test_predicates_escape_exact_seasons_and_validate_ids():
    pred = audit.scope_predicate([{"competition_id": "2", "source_season_key": " 2008 O'Brien | Final "}])
    assert "' 2008 O''Brien | Final '" in pred
    assert audit.in_predicate("match_id", [3, 1, 1], True) == "match_id IN (1,3)"
    with pytest.raises(audit.AuditError):
        audit.scope_predicate([{"competition_id": "1 OR TRUE", "source_season_key": "2008"}])
    with pytest.raises(audit.AuditError):
        audit.in_predicate("arbitrary", [1])


@pytest.mark.parametrize("sql", ["DELETE FROM t", "SELECT 1; SELECT 2", "WITH t AS (DELETE FROM x) SELECT 1",
                                  "EXPLAIN ANALYZE SELECT 1", "CALL foo()", "SELECT * FROM a.b_current",
                                  "SELECT 1 -- hidden", "SELECT 1 /* hidden */", "SELECT * FROM iceberg.bronze.foo"])
def test_readonly_guard_rejects(sql):
    with pytest.raises(audit.AuditError):
        audit.guard_sql(sql)


def test_readonly_guard_accepts_literals_and_enforces_utf8_size():
    assert audit.guard_sql("SELECT 'delete; insert' AS text")
    assert audit.guard_sql("SELECT * FROM iceberg.bronze.foo FOR VERSION AS OF 1")
    with pytest.raises(audit.QueryError, match="SQLTooLarge"):
        audit.guard_sql("SELECT '" + "я" * 65536 + "'")


def snapshots():
    return {key: {"snapshot_id": i + 1, "made_current_at": "2026-11-16T00:00:00Z"}
            for i, key in enumerate(("matches", "payloads", "manifest", "c1"))}


def test_all_templates_render_and_are_read_only():
    audit.validate_templates(snapshots())
    for name in ("current_payloads", "current_payload_ids"):
        sql = audit.render(name, snapshots(), scope_predicate="competition_id = 2",
                           manifest_scope_predicate="competition_id = '2'",
                           selected_batch_predicate="FALSE", match_id_predicate="FALSE")
        assert "competition_id = '2'" in sql
        assert "competition_id = 2 AND" not in sql
        if name == "current_payloads":
            assert "'{}'" in sql and "AS raw_root" in sql
            assert "payload_json_valid" in sql
            assert sql.count('AS "raw_') == 6 and sql.count('AS "typed_') == 6
            assert "SELECT payload_json" not in sql


def test_query_failure_is_durably_logged_without_messages(tmp_path):
    class Fake:
        def execute(self, sql):
            raise audit.QueryError("SnapshotExpired", "synthetic-query-id")
    queries = audit.Queries(Fake(), tmp_path / "ledger.csv")
    with pytest.raises(audit.QueryError):
        queries.run("SELECT 1", "test")
    row = audit.read_csv(tmp_path / "ledger.csv")[0]
    assert row["query_id"] == "synthetic-query-id" and row["status"] == "failed"
    assert row["error_class"] == "SnapshotExpired"


def test_query_deadline_fails_closed_and_records_id(tmp_path, monkeypatch):
    class Fake:
        def execute(self, sql):
            return {"query_id": "synthetic", "rows": []}
    clock = iter([0, 0, 26, 26])
    monkeypatch.setattr(audit.time, "monotonic", lambda: next(clock))
    query = audit.Queries(Fake(), tmp_path / "ledger.csv")
    with pytest.raises(audit.QueryError, match="StatementBudgetExceeded"):
        query.run("SELECT 1", "timeout")
    assert audit.read_csv(tmp_path / "ledger.csv")[0]["query_id"] == "synthetic"


def test_expired_snapshot_never_splits_or_repins(monkeypatch):
    calls = []
    def fail(*args):
        calls.append(1)
        raise audit.QueryError("SnapshotExpired")
    monkeypatch.setattr(audit, "read_scope_chunk", fail)
    with pytest.raises(audit.QueryError):
        audit.fetch_scopes(None, snapshots(), [[manifest(), manifest(competition_id=2)]], True)
    assert len(calls) == 1


def test_calibration_splits_but_frozen_pass_fails(monkeypatch):
    calls = []
    def fake(queries, snaps, scopes, label):
        calls.append(len(scopes))
        if len(scopes) > 1:
            raise audit.QueryError("ServerTimeout")
        return {}, {}, []
    monkeypatch.setattr(audit, "read_scope_chunk", fake)
    values = [[manifest(), manifest(competition_id=2)]]
    final, _, _, _ = audit.fetch_scopes(None, snapshots(), values, True)
    assert len(final) == 2 and calls == [2, 1, 1]
    with pytest.raises(audit.QueryError):
        audit.fetch_scopes(None, snapshots(), values, False)


def test_chunks_stable_and_pass_id_cannot_escape():
    assert audit.chunks(list(range(51)), 25) == [list(range(25)), list(range(25, 50)), [50]]
    for name in ("../escape", "", "/tmp/foo", "a b"):
        with pytest.raises(audit.AuditError):
            audit.validate_name(name)


class SyntheticTrino:
    def __init__(self, late=False, interrupted=False, match_changes=None, payload_changes=None):
        self.calls = []
        self.late = late
        self.interrupted = interrupted
        self.match_changes = match_changes or {}
        self.payload_changes = payload_changes or {}

    def execute(self, sql):
        audit.guard_sql(sql)
        self.calls.append(sql)
        if "$history" in sql:
            rows = [{"table_key": key, **row} for key, row in snapshots().items()]
        elif "target_type = 'scope_completion'" in sql:
            rows = [manifest(batch="marker", target_key="scope:1", status="interrupted" if self.interrupted else "success",
                             capabilities_json='{"coverage":{"scope_entities":["matches"]}}')]
        elif "target_type = 'scope_source_gap'" in sql:
            rows = []
        elif "fotmob_c1_inputs" in sql:
            rows = [c1_input(), {"kind": "control", "deadline_day": "2026-11-16"}]
        elif "target_type = 'league_season'" in sql:
            rows = [manifest(cid, batch=f"calendar-{cid}", competition_id=cid) for cid in range(1, 19)]
        elif "SELECT DISTINCT competition_id" in sql:
            rows = [{"competition_id": cid, "source_season_key": "2008 Final Stage", "_target_batch_id": f"calendar-{cid}"}
                    for cid in range(1, 19)]
        elif "fotmob_matches FOR" in sql:
            rows = [match(cid, f"calendar-{cid}", competition_id=cid,
                          **self.match_changes.get(cid, {})) for cid in range(1, 19)]
        elif "WITH bucket_rows" in sql:
            import zlib
            bucket = int(re.search(r"16\) = ([0-9]+)", sql).group(1))
            rows = [manifest(cid, batch=f"payload-{cid}", competition_id=cid, late_failure=self.late)
                    for cid in range(1, 19) if zlib.crc32(str(cid).encode()) % 16 == bucket]
        elif "fotmob_match_payloads FOR" in sql:
            rows = [payload(cid, f"payload-{cid}", competition_id=cid,
                            **self.payload_changes.get(cid, {})) for cid in range(1, 19)]
        elif "target_type = 'match'" in sql:
            rows = []
        else:
            raise AssertionError(sql)
        return {"query_id": f"synthetic_{len(self.calls):04d}", "rows": rows}


@pytest.fixture
def collected(tmp_path):
    registry, historical = inputs(tmp_path)
    run = tmp_path / "run"
    transport = SyntheticTrino(late=True, interrupted=True)
    audit.plan(argparse.Namespace(registry=registry, historical=historical, run_dir=run, required_scopes=18,
                                  bucket_count=16), transport)
    for name, calibrate in (("calibration", True), ("pass1", False), ("pass2", False)):
        audit.collect(argparse.Namespace(run_dir=run, pass_id=name, calibrate=calibrate), transport)
    return run


def test_full_synthetic_pipeline_reports_and_replays(collected, tmp_path):
    report = tmp_path / "report"
    result = audit.verify(argparse.Namespace(run_dir=collected, pass_a="pass1", pass_b="pass2", report_dir=report))
    assert result["status"] == "PASS"
    assert audit.load(report / "replay-comparison.json")["status"] == "PASS"
    assert audit.load(report / "run-manifest.json")["local_evidence"]["rows"] == 18
    marker = audit.read_csv(report / "marker-results.csv")[0]
    assert marker["category"] == "unknown"
    reasons = json.loads(marker["reason_counts"])
    assert reasons["unknown:latest_marker_incomplete"] == 1
    assert reasons["confirmed_content:current_payload_present"] == 1
    evidence = audit.read_csv(collected / "passes/pass1/current-id-evidence.csv")
    assert all(row["payload_present"] == "1" and row["late_failure"] == "1" for row in evidence)
    assert "not current" in (report / "README.md").read_text()
    assert audit.read_csv(report / "section-mismatches.csv") == []
    section_path = collected / "passes/pass1/section-evidence.csv"
    assert len(audit.read_csv(section_path)) == 18 * 6
    local = audit.load(report / "run-manifest.json")["local_section_evidence"]
    assert local == {"path": str(section_path), "sha256": audit.file_sha(section_path), "rows": 18 * 6}
    for name in ("pass1", "pass2"):
        directory = collected / "passes" / name
        hashes = audit.load(directory / "content-hashes.json")
        for artifact in ("section-evidence.csv", "section-mismatches.csv"):
            assert hashes[artifact] == audit.file_sha(directory / artifact)


def test_calibration_cannot_be_reported(collected):
    with pytest.raises(audit.AuditError, match="non-calibration"):
        audit.checked_pass(collected, "calibration")


def test_input_hash_drift_refused(collected):
    path = Path(audit.load(collected / "input-hashes.json")["registry"]["path"])
    path.write_text(path.read_text() + "\n")
    with pytest.raises(audit.AuditError, match="input bytes changed"):
        audit.check_frozen(collected)


def test_snapshots_never_repinned(collected):
    path = collected / "snapshots.json"
    saved = audit.load(path)
    saved["matches"]["snapshot_id"] += 1
    audit.dump(path, saved)
    with pytest.raises(audit.AuditError, match="frozen plan changed"):
        audit.check_frozen(collected)


def test_replay_compares_content_ignoring_query_ids_and_timing(collected):
    a, b = collected / "passes/pass1", collected / "passes/pass2"
    assert audit.compare_content(a, b)["status"] == "PASS"
    with (b / "query-ledger.csv").open("a") as stream:
        stream.write("ignored by byte comparison\n")
    assert audit.compare_content(a, b)["status"] == "PASS"
    with (b / "scope-summary.csv").open("a") as stream:
        stream.write("changed content\n")
    assert audit.compare_content(a, b)["status"] == "FAIL"


@pytest.mark.parametrize("mutation,check", [("arithmetic", "finished_arithmetic"), ("scope", "all_required_scopes_once"),
                                           ("marker", "all_latest_marker_targets_once"), ("historical", "historical_18_once"),
                                           ("budget", "pass_budget"), ("query", "all_queries_successful"),
                                           ("section", "six_sections_every_current_payload")])
def test_report_validation_fails_closed(collected, mutation, check):
    directory = collected / "passes/pass1"
    if mutation in {"arithmetic", "scope"}:
        path = directory / "scope-summary.csv"
        rows = audit.read_csv(path)
        if mutation == "arithmetic":
            rows[0]["finished"] = "999"
        else:
            rows.pop()
        put_csv(path, audit.SUMMARY_FIELDS, rows)
    elif mutation in {"marker", "historical", "section"}:
        path = directory / {"marker": "marker-results.csv", "historical": "historical-1294-results.csv",
                            "section": "section-evidence.csv"}[mutation]
        rows = audit.read_csv(path)
        fields = tuple(rows[0])
        put_csv(path, fields, rows[1:])
        refresh_content_hash(directory, path)
    elif mutation == "budget":
        timing = audit.load(directory / "timing.json")
        timing["pass_wall_seconds"] = audit.LIMITS["pass_seconds"] + 1
        audit.dump(directory / "timing.json", timing)
    else:
        rows = audit.read_csv(directory / "query-ledger.csv")
        rows[0]["status"] = "failed"
        put_csv(directory / "query-ledger.csv", audit.LEDGER_FIELDS, rows)
    result = audit.validate_report(collected, directory)
    assert result["status"] == "FAIL" and result["checks"][check] is False


def c1_input(mid=1, **extra):
    return {"kind": "input", "competition_id": 1, "source_season_key": "2008 Final Stage",
            "match_id": mid, "deadline_day": "2026-11-16", "deadline": "2026-11-15T00:00:00Z",
            "first_ft": "2026-11-14T00:00:00Z", "first_ingested": None,
            "na_runs": 0, "na_spread_s": 0, "success_count": 0, **extra}


def c1_evidence(mid=1, present=False, **extra):
    row = {**match(mid), "payload_present": present,
           "category": "confirmed_content" if present else "unknown", "evidence_ids": [], **extra}
    row["scoped_id"] = audit.scoped_id(row)
    return row


def test_c1_is_limited_ever_ingested_and_keeps_late_difference_ids():
    c1 = [c1_input(), c1_input(2, first_ingested="2026-01-01T00:00:00Z"), c1_input(3),
          {"kind": "control", "match_id": None}]
    evidence = [c1_evidence(1, True), c1_evidence(2)]
    result = audit.reconcile_c1(c1, evidence, snapshots(), CAPTURED_AT)
    assert result["c1_no_payload"] == 2 and result["current_no_payload_comparable"] == 1
    assert result["late_correction_current_added_ids"] == [audit.scoped_id(match(1))]
    assert result["ever_ingested_but_not_current_ids"] == [audit.scoped_id(match(2))]
    assert result["outside_audit_calendar_ids"] == [audit.scoped_id(match(3))]
    assert result["input_rows"] == 3 and result["control_rows"] == 1


def test_literal_cancelled_gap_retained_separately(tmp_path):
    registry, historical = inputs(tmp_path)
    scopes = list(audit.load_registry(registry, 18).values())
    for scope in scopes:
        scope["historical_1294"] = True
    calendar = audit.deduplicate([match(cancelled=True)])
    artifacts = audit.make_artifacts({"scopes": scopes, "historical": audit.load_historical(historical),
                                     "captured_at": CAPTURED_AT}, [], [], [],
                                    snapshots(), {(1, "2008 Final Stage"): manifest()}, calendar, {}, {}, {}, [])
    summary = artifacts["scope-summary.csv"][1][0]
    assert summary["finished"] == summary["without_payload"] == 1
    assert summary["actionable_gap"] == 0
    assert artifacts["mismatched-ids.csv"][1][0]["reason"] == "cancelled"


def test_malformed_candidate_flags_calendar_even_if_success_has_rows():
    row = manifest(capabilities_json="{broken")
    winners, anomalies = audit.choose_calendar([row], {(1, "2008 Final Stage", "b1")})
    assert winners[(1, "2008 Final Stage")]["batch_id"] == "b1"
    assert anomalies[0]["reason"] == "malformed_candidate_capabilities"
    assert audit.classify_missing(match(), row, [row])[0] == "unknown"


def test_invalid_mask_does_not_hide_physical_writer_failure():
    row = payload(available_sections="malformed", typed_stats=False)
    stats = next(item for item in audit.section_results(row, True) if item["section"] == "stats")
    assert stats["category"] == "writer_failed"
    row = payload(available_sections={"stats": "false"})
    assert all(item["category"] == "unknown" for item in audit.section_results(row, True))


def test_old_unrelated_schema_failure_does_not_relabel_current_omission():
    row = payload(raw_matchFacts=False, typed_matchFacts=False,
                  _observed_at="2026-11-16T00:00:00Z",
                  available_sections=[name for name in audit.SECTIONS if name != "matchFacts"])
    assert audit.section_results(row, True, [manifest(status="schema_drift", batch="old")])[0]["category"] == "source_unavailable"


def test_invalid_marker_keeps_physical_work_scope():
    row = manifest(capabilities_json="{malformed")
    assert audit.marker_scopes(row)[1] is False
    assert audit.marker_work_scopes(row) == [{"competition_id": 1, "source_season_key": "2008 Final Stage"}]


def test_transport_contract_without_running_subprocess(monkeypatch):
    seen = []
    def fake(argv, **kwargs):
        seen.append((argv, kwargs))
        return argparse.Namespace(returncode=0, stdout=json.dumps({"ok": True, "query_id": "synthetic-q",
                                                                   "columns": ["number"], "rows": [[1]]}), stderr="")
    monkeypatch.setattr(audit.subprocess, "run", fake)
    result = audit.RemoteTrino("run-2256").execute("SELECT 1")
    assert result == {"query_id": "synthetic-q", "rows": [{"number": 1}]}
    argv, options = seen[0]
    assert argv[:8] == ["docker", "exec", "--workdir", "/tmp", "airflow-scheduler", "python", "-B", "-W"]
    assert argv[-2:] == ["SELECT 1", "run-2256"]
    assert options["timeout"] == 35 and options["capture_output"]
    assert "request_timeout=(3, 30)" in audit.REMOTE_SCRIPT
    assert '"query_max_run_time": "25s"' in audit.REMOTE_SCRIPT
    assert "cursor.query_id" in audit.REMOTE_SCRIPT


def test_transport_does_not_expose_stderr_or_exception_message(monkeypatch):
    monkeypatch.setattr(audit.subprocess, "run", lambda *args, **kwargs: argparse.Namespace(
        returncode=1, stdout="", stderr="synthetic-sensitive-string"))
    with pytest.raises(audit.QueryError) as failure:
        audit.RemoteTrino("run").execute("SELECT 1")
    assert "synthetic-sensitive-string" not in str(failure.value)


def test_collector_only_reads_full_history_for_selected_problem_ids(collected):
    frozen = audit.load(collected / "chunk-plan.json")
    # All synthetic sections agree and all scoped payloads are present. A late
    # failure summary from the global scan needs no full matching-ID chronology.
    assert frozen["history_batches"] == []
    ledger = audit.read_csv(collected / "passes/pass1/query-ledger.csv")
    assert not [row for row in ledger if row["step"] == "match_evidence"]


def test_incomplete_pass_refused_even_when_content_exists(collected):
    path = collected / "passes/pass1/status.json"
    status = audit.load(path)
    status["complete"] = False
    audit.dump(path, status)
    with pytest.raises(audit.AuditError, match="complete"):
        audit.checked_pass(collected, "pass1")


def test_frozen_chunk_change_refused(collected):
    path = collected / "chunk-plan.json"
    value = audit.load(path)
    value["bucket_count"] = 32
    audit.dump(path, value)
    with pytest.raises(audit.AuditError, match="frozen"):
        audit.collect(argparse.Namespace(run_dir=collected, pass_id="third", calibrate=False), SyntheticTrino())


def test_replay_mismatch_is_saved_and_fails(collected, tmp_path):
    directory = collected / "passes/pass2"
    path = directory / "candidate-anomalies.json"
    audit.dump(path, [{"reason": "synthetic anomaly"}])
    hashes = audit.load(directory / "content-hashes.json")
    hashes[path.name] = audit.file_sha(path)
    audit.dump(directory / "content-hashes.json", hashes)
    report = tmp_path / "failed-report"
    with pytest.raises(audit.AuditError, match="replay comparison failed"):
        audit.verify(argparse.Namespace(run_dir=collected, pass_a="pass1", pass_b="pass2", report_dir=report))
    assert audit.load(report / "replay-comparison.json")["status"] == "FAIL"
    assert audit.load(report / "validation.json")["status"] == "FAIL"


def test_deliverable_registry_has_exact_required_scopes_and_labels():
    path = SCRIPT.parents[2] / "docs/research/fotmob-scope-2026-11-14/scope-registry.csv"
    before = path.read_bytes()
    raw = audit.read_csv(path)
    included = [row for row in raw if row["decision"] == "included"]
    expected = {(int(row["competition_id"]), season): row["catalog_name"]
                for row in included for season in row["required_season_keys"].split("|")}
    loaded = audit.load_registry(path, 5540)
    assert len(included) == len({key[0] for key in loaded}) == 452
    assert len(expected) == len(loaded) == 5540
    assert set(loaded) == set(expected)
    assert all(row["catalog_name"] == expected[key] for key, row in loaded.items())
    assert path.read_bytes() == before


def test_deliverable_historical_preserves_exact_keys_labels_and_signatures():
    path = SCRIPT.parents[2] / "docs/research/fotmob-completion-audit-2026-11-16/historical-1294-input.csv"
    before = path.read_bytes()
    raw = {audit.scope_key(row): row for row in audit.read_csv(path)}
    loaded = audit.load_historical(path)
    assert len(raw) == len(loaded) == 18
    assert sum(row["old_gap"] for row in loaded) == 1762
    assert {audit.scope_key(row) for row in loaded} == set(raw)
    for row in loaded:
        original = raw[audit.scope_key(row)]
        assert row["source_season_key"] == original["source_season_key"]
        assert row["competition_name"] == original["competition_name"]
        assert row["historical_parser_and_plan_signatures"] == original["historical_parser_and_plan_signatures"]
        assert row["old_gap"] == int(original["historical_finished_without_details"])
        assert row["baseline_row"] == original
    assert path.read_bytes() == before


def test_manifest_scope_predicate_uses_varchar_ids_and_exact_labels():
    scopes = [{"competition_id": "2", "source_season_key": " 2008 O'Brien | Final "}]
    physical = audit.scope_predicate(scopes)
    manifest_predicate = audit.manifest_scope_predicate(scopes)
    assert "competition_id = 2" in physical and "competition_id = '2'" not in physical
    assert "competition_id = '2'" in manifest_predicate
    assert "source_season_key = ' 2008 O''Brien | Final '" in physical
    assert "source_season_key = ' 2008 O''Brien | Final '" in manifest_predicate
    assert audit.manifest_scope_predicate([]) == "FALSE"
    assert audit.manifest_scope_predicate(scopes * 2) == manifest_predicate
    with pytest.raises(audit.AuditError):
        audit.manifest_scope_predicate([{"competition_id": "2 OR TRUE", "source_season_key": "2008"}])
    sql = audit.render("league_manifest", snapshots(), scope_predicate=physical,
                       manifest_scope_predicate=manifest_predicate)
    assert manifest_predicate in sql


def test_payload_templates_selected_only_for_marker_or_historical_chunks():
    historical = {"competition_id": 1, "source_season_key": "2008 Final Stage"}
    marker = manifest(competition_id=2, capabilities_json='{"coverage":{"scope_entities":["matches"]}}')
    ordinary = {"competition_id": 3, "source_season_key": " 2008 O'Brien "}
    selected = audit.section_scope_keys({"historical": [historical]}, [marker])
    assert selected == {(1, "2008 Final Stage"), (2, "2008 Final Stage")}
    groups = [[historical], [marker], [ordinary]]
    modes = audit.payload_modes(groups, selected)
    assert modes == ["current_payloads", "current_payloads", "current_payload_ids"]
    calls = []

    class Fake:
        def run(self, sql, step, **kwargs):
            calls.append((sql, step, kwargs))
            return []

    for index, (scopes, mode) in enumerate(zip(groups, modes)):
        audit.fetch_payload_batch(Fake(), snapshots(), scopes, [1], {1: manifest()}, str(index), mode)
    assert [step for _, step, _ in calls] == modes
    for sql, step, _ in calls:
        assert "FOR VERSION AS OF" in sql
        if step == "current_payload_ids":
            assert "payload_json" not in sql
            assert "AS raw_root" not in sql and 'AS "raw_' not in sql
        else:
            assert "AS raw_root" in sql
    with pytest.raises(audit.AuditError, match="invalid payload mode"):
        audit.fetch_payload_batch(Fake(), snapshots(), [ordinary], [1], {1: manifest()}, "bad", "other")
    assert len(calls) == 3


def test_payload_modes_and_query_steps_are_frozen(collected):
    pinned = audit.load(collected / "snapshots.json")
    frozen = audit.check_chunk_plan(collected, pinned)
    assert frozen["payload_modes"] == ["current_payloads"] * len(frozen["scope_chunks"])
    for name in ("pass1", "pass2"):
        result = audit.validate_report(collected, collected / "passes" / name)
        for check in ("payload_modes_frozen", "payload_query_modes", "payload_id_chunks_reconcile",
                      "present_payload_queries_covered", "history_id_chunks_reconcile"):
            assert result["checks"][check] is True
    # Even resealing a changed mode cannot make it consistent with the frozen scopes.
    frozen["payload_modes"][0] = "current_payload_ids"
    path = collected / "chunk-plan.json"
    audit.dump(path, frozen)
    audit.dump(collected / "chunk-plan-seal.json", {"sha256": audit.file_sha(path)})
    with pytest.raises(audit.AuditError, match="chunk plan"):
        audit.check_chunk_plan(collected, pinned)


def test_id_only_payload_chunks_replay_the_frozen_template(monkeypatch):
    scopes = [[{"competition_id": cid, "source_season_key": "2008 Final Stage"}] for cid in (1, 2)]
    modes = audit.payload_modes(scopes, {(1, "2008 Final Stage")})
    calendar = audit.deduplicate([match(1), match(2, competition_id=2)])
    winners = {1: manifest(1), 2: manifest(2, competition_id=2)}
    frozen = [[[1]], [[2]]]
    calls = []

    def fetch(queries, snaps, selected, ids, selected_winners, label, mode):
        calls.append((label, mode, list(ids)))
        cid = selected[0]["competition_id"]
        return [payload(mid, competition_id=cid) for mid in ids]

    monkeypatch.setattr(audit, "fetch_payload_batch", fetch)
    first = audit.fetch_payloads(None, snapshots(), scopes, calendar, winners, frozen, False, modes)
    second = audit.fetch_payloads(None, snapshots(), scopes, calendar, winners, frozen, False, modes)
    assert first == second
    assert calls == [("0.0", "current_payloads", [1]), ("1.0", "current_payload_ids", [2])] * 2
    with pytest.raises(audit.AuditError, match="payload plan length"):
        audit.fetch_payloads(None, snapshots(), scopes, calendar, winners, frozen, False, modes[:1])


@pytest.mark.parametrize("artifact", ["section-evidence.csv", "section-mismatches.csv"])
def test_section_artifacts_are_in_replay_content(collected, artifact):
    a, b = collected / "passes/pass1", collected / "passes/pass2"
    assert audit.compare_content(a, b)["status"] == "PASS"
    path = b / artifact
    path.write_bytes(path.read_bytes() + b"\n")
    refresh_content_hash(b, path)
    assert audit.compare_content(a, b)["status"] == "FAIL"


def test_section_disagreement_is_exact_mismatch_subset(tmp_path):
    registry, historical = inputs(tmp_path)
    run = tmp_path / "run"
    transport = SyntheticTrino(payload_changes={1: {"typed_stats": False}})
    audit.plan(argparse.Namespace(registry=registry, historical=historical, run_dir=run,
                                  required_scopes=18, bucket_count=16), transport)
    for name, calibrate in (("calibration", True), ("pass1", False), ("pass2", False)):
        audit.collect(argparse.Namespace(run_dir=run, pass_id=name, calibrate=calibrate), transport)
    directory = run / "passes/pass1"
    sections = audit.read_csv(directory / "section-evidence.csv")
    mismatches = audit.read_csv(directory / "section-mismatches.csv")
    assert len(sections) == 18 * 6
    assert mismatches == [row for row in sections if row["category"] != "confirmed_content"]
    assert len(mismatches) == 1
    assert mismatches[0]["section"] == "stats" and mismatches[0]["category"] == "writer_failed"
    ids = audit.read_csv(directory / "mismatched-ids.csv")
    assert [row["match_id"] for row in ids] == ["1"]
    assert audit.validate_report(run, directory)["status"] == "PASS"
    assert audit.compare_content(directory, run / "passes/pass2")["status"] == "PASS"


def test_c1_planning_shape_filters_first_ft_and_preserves_three_scoped_unknown_gaps():
    # One global ID appears in three independent scopes; day/control records are not inputs.
    gaps = [c1_input(7, competition_id=cid) for cid in (1, 2, 3)]
    unfinished = c1_input(8, first_ft=None)
    ingested = c1_input(9, first_ingested="2026-11-14T01:00:00Z", success_count=1)
    c1 = gaps + [unfinished, ingested,
                 {"kind": "control", "deadline_day": "2026-11-16", "match_id": 999},
                 {"kind": "day", "deadline_day": "2026-11-16", "match_id": 998}]
    evidence = [c1_evidence(7, competition_id=cid) for cid in (1, 2, 3)]
    evidence += [c1_evidence(8, category="not_applicable", evidence_ids=["calendar"]), c1_evidence(9, True)]
    result = audit.reconcile_c1(c1, evidence, snapshots(), CAPTURED_AT)
    expected = sorted(audit.scoped_id(row) for row in gaps)
    assert result["cached_input_rows"] == result["input_rows"] == result["distinct_input_ids"] == 5
    assert result["distinct_input_global_ids"] == 3
    assert result["control_rows"] == 2 and result["unresolved_rows"] == 0
    assert result["first_ft_known_finished"] == 4
    assert result["finished_without_ever_ingested"] == 3
    assert result["source_unavailable_threshold"] == 0
    assert result["c1_no_payload"] == 4  # Includes the unfinished cache input only in this inventory.
    for key in ("finished_without_ever_ingested_scoped_ids", "c1_unconfirmed_scoped_ids",
                "c1_no_absence_evidence_scoped_ids", "unconfirmed_scoped_ids", "no_evidence_scoped_ids"):
        assert result[key] == expected
        assert result["set_hashes"][key] == audit.id_set_hash(expected)
    assert audit.scoped_id(unfinished) not in result["first_ft_known_finished_scoped_ids"]
    assert result["deadline_days_counts"] == {"2026-11-16": 5}
    assert result == audit.reconcile_c1(list(reversed(c1)), list(reversed(evidence)), snapshots(), CAPTURED_AT)


@pytest.mark.parametrize("runs,spread,success,unavailable", [
    (1, 10000, 0, False), (2, 7199, 0, False), (2, 7200, 0, True),
    (3, 0, 0, True), (3, 7200, 1, False),
])
def test_c1_source_unavailable_thresholds_require_no_success(runs, spread, success, unavailable):
    row = c1_input(na_runs=runs, na_spread_s=spread, success_count=success)
    result = audit.reconcile_c1([row], [c1_evidence()], snapshots(), CAPTURED_AT)
    assert result["source_unavailable_threshold"] == int(unavailable)
    assert result["source_unavailable_threshold_scoped_ids"] == ([audit.scoped_id(row)] if unavailable else [])
    assert result["deadline_reached_scoped_ids"] == [audit.scoped_id(row)]


def test_c1_invalid_input_keeps_scoped_unknown_evidence():
    row = c1_input(first_ft="invalid")
    result = audit.reconcile_c1([row], [], snapshots(), CAPTURED_AT)
    assert result["cached_input_rows"] == result["unresolved_rows"] == 1
    assert result["input_rows"] == result["distinct_input_ids"] == 0
    assert result["unresolved_input_scoped_ids"] == result["no_evidence_scoped_ids"] == [audit.scoped_id(row)]


@pytest.mark.parametrize("winner", [None, manifest()])
def test_unfinished_applicability_precedes_writer_failure(winner):
    assert audit.classify_missing(match(finished=False), winner, []) == ("not_applicable", "unfinished")
    assert audit.classify_missing(match(finished=False, awarded=True), winner, []) == ("unknown", "awarded_unfinished")


def test_successful_current_winner_writer_gap_precedes_late_retryable_failure():
    old = manifest(hour=1)
    failure = manifest(status="retryable_failure", fetch_outcome="retryable_failure", hour=2)
    assert audit.classify_missing(match(), old, [old, failure]) == ("writer_failed", "expected_current_payload_missing")
    assert audit.classify_missing(match(), old, [manifest(status="retryable_failure", hour=0)]) == (
        "writer_failed", "expected_current_payload_missing")


def artifact_fixture(tmp_path, matches, payload_rows=(), markers=(), winners=None):
    registry, historical = inputs(tmp_path)
    scopes = list(audit.load_registry(registry, 18).values())
    for scope in scopes:
        scope["historical_1294"] = True
    plan_data = {"scopes": scopes, "historical": audit.load_historical(historical), "captured_at": CAPTURED_AT}
    calendar = audit.deduplicate(matches)
    current = audit.deduplicate(payload_rows)
    return audit.make_artifacts(plan_data, list(markers), [], [], snapshots(),
                                {(1, "2008 Final Stage"): manifest()}, calendar,
                                winners or {}, current, {}, [])


@pytest.mark.parametrize("with_payload", [False, True])
def test_ordinary_unfinished_never_poisons_marker_or_mismatches(tmp_path, with_payload):
    marker = manifest(target_key="scope:1", expected_counts_json='{"matches":1}',
                      capabilities_json='{"coverage":{"scope_entities":["matches"]}}')
    current = [payload(finished=False, typed_stats=False)] if with_payload else []
    artifacts = artifact_fixture(tmp_path, [match(finished=False)], current, [marker],
                                 {1: manifest(late_failure=True)})
    row = artifacts["current-id-evidence.csv"][1][0]
    assert row["category"] == "not_applicable" and row["reason"] == "unfinished"
    assert artifacts["mismatched-ids.csv"][1] == []
    summary = artifacts["scope-summary.csv"][1][0]
    assert summary["completion_reason_counts"] == summary["section_reason_counts"] == {}
    result = artifacts["marker-results.csv"][1][0]
    assert result["category"] == "not_applicable"
    assert result["reason_counts"] == {"not_applicable:no_finished_matches": 1}
    assert len(artifacts["section-evidence.csv"][1]) == (6 if with_payload else 0)
    if with_payload:
        assert artifacts["section-mismatches.csv"][1]  # Physical evidence remains visible.


@pytest.mark.parametrize("expected,category,reason", [
    (0, "not_applicable", "advertised_empty_scope"), (1, "unknown", "empty_current_calendar"),
])
def test_marker_empty_calendar_requires_explicit_zero_expectation(tmp_path, expected, category, reason):
    marker = manifest(target_key="scope:1", expected_counts_json=json.dumps({"matches": expected}),
                      capabilities_json='{"coverage":{"scope_entities":["matches"]}}')
    artifacts = artifact_fixture(tmp_path, [], markers=[marker])
    result = artifacts["marker-results.csv"][1][0]
    assert result["category"] == category
    assert result["reason_counts"] == {category + ":" + reason: 1}


def test_last_good_payload_remains_present_with_late_failure(tmp_path):
    artifacts = artifact_fixture(tmp_path, [match()], [payload()], winners={1: manifest(late_failure=True)})
    row = artifacts["current-id-evidence.csv"][1][0]
    assert row["payload_present"] and row["late_failure"]
    assert row["category"] == "confirmed_content"
    assert artifacts["mismatched-ids.csv"][1] == [row]


def test_future_uses_utc_time_plus_three_hours_and_single_frozen_capture(tmp_path, monkeypatch):
    class Clock(datetime):
        wall = datetime(2026, 11, 16, tzinfo=timezone.utc)

        @classmethod
        def now(cls, tz=None):
            return cls.wall if tz is not None else cls.wall.replace(tzinfo=None)

    monkeypatch.setattr(audit, "datetime", Clock)
    registry, historical = inputs(tmp_path)
    run = tmp_path / "run"
    transport = SyntheticTrino(match_changes={
        1: {"utc_time": "2026-11-15T21:00:01Z"},
        2: {"utc_time": "2026-11-15T21:00:00Z"},
        3: {"utc_time": "2026-11-15T20:59:59Z"},
        4: {"utc_time": None},
    })
    audit.plan(argparse.Namespace(registry=registry, historical=historical, run_dir=run,
                                  required_scopes=18, bucket_count=16), transport)
    captured = audit.load(run / "scope-plan.json")["captured_at"]
    assert audit.timestamp(captured) == audit.timestamp(CAPTURED_AT)
    audit.collect(argparse.Namespace(run_dir=run, pass_id="calibration", calibrate=True), transport)
    for name, wall in (("pass1", datetime(2026, 11, 17, tzinfo=timezone.utc)),
                       ("pass2", datetime(2027, 1, 1, tzinfo=timezone.utc))):
        Clock.wall = wall
        audit.collect(argparse.Namespace(run_dir=run, pass_id=name, calibrate=False), transport)
        directory = run / "passes" / name
        rows = {int(row["match_id"]): row for row in audit.read_csv(directory / "current-id-evidence.csv")}
        assert rows[1]["future"] == "1"
        assert rows[1]["category"] == "not_applicable" and rows[1]["reason"] == "future"
        assert all(rows[mid]["future"] == "0" and rows[mid]["category"] == "confirmed_content" for mid in (2, 3, 4))
        assert audit.validate_report(run, directory)["status"] == "PASS"
    a, b = run / "passes/pass1", run / "passes/pass2"
    assert audit.compare_content(a, b)["status"] == "PASS"
    assert audit.load(a / "content-hashes.json") == audit.load(b / "content-hashes.json")
    for artifact in audit.load(a / "content-hashes.json"):
        assert (a / artifact).read_bytes() == (b / artifact).read_bytes()
