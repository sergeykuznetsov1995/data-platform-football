import json
from datetime import timedelta

import duckdb
import pytest
import sqlglot

from scripts.report_fbref_lineup_coverage import ROOT, instant, prepare

START = instant("2026-01-01T15:00:00Z")
END = START + timedelta(days=14)
MATCH = "deadbeef"
DIRECT = "cafebabe"
MISSING = "00aabbcc"
SQL = ROOT / "docs/operations/sql/fbref_lineups_day14.sql"


def snapshot():
    return {
        "coverage": {"start": "2026-01-01", "end": "2026-01-15", "as_of": END.isoformat()},
        "competitions": [{"competition_id": "9", "gender": "male", "name": "Premier League",
                          "present": True, "crawl_state": "active", "lifecycle_state": "present"}],
        "seasons": [{"competition_id": "9", "season_id": "2026", "is_current": True,
                     "present": True, "lifecycle_state": "present"}],
        "schedule_health": [{"competition_id": "9", "season_id": "2026", "fetched": True}],
        "schedules": [], "observations": [], "errors": [],
    }


def control(editions=()):
    return {"as_of": END.isoformat(), "cutoff_receipt": "fixture receipt", "editions": list(editions)}


def fixture(mid, **kwargs):
    return {"source_competition_id": "9", "source_season_id": "2026", "date": "2026-01-08",
            "time": "12:00", "home": "A", "away": "B", "score": "0–0", "notes": "",
            "match_url": f"/en/matches/{mid}" if mid else None, **kwargs}


def observation(mid, kickoff):
    return {"competition_id": "9", "season_id": "2026", "match_id": mid,
            "kickoff_at": kickoff.isoformat(), "kickoff_observed_at": END.isoformat(),
            "first_completed_seen_at": END.isoformat(), "first_completed_raw_key": f"raw/{mid}"}


def test_window_signed_score_and_unreported_deduplication():
    snap = snapshot()
    snap["schedules"] = [fixture(MATCH), fixture(MATCH), fixture(DIRECT), fixture(MISSING, notes="awarded"),
                         fixture(None, home="C"), fixture(None, home="C"), fixture(None, score="")]
    snap["observations"] = [observation(MATCH, START), observation(DIRECT, END)]
    packet = prepare(snap, control(), START, END)
    assert {r[2] for r in packet["fixtures"]} == {MATCH, None}
    assert len(packet["fixtures"]) == 2
    assert packet["issues"] == ["unconfirmed_kickoff", "unreported_fixture"]
    with pytest.raises(ValueError, match="14 days"):
        prepare(snap, control(), START, END + timedelta(seconds=1))


@pytest.mark.parametrize("broken", [
    {"first_completed_raw_key": None}, {"first_completed_seen_at": None},
    {"kickoff_observed_at": None}, {"kickoff_observed_at": (END + timedelta(seconds=1)).isoformat()},
    {"kickoff_at": None}, {"first_completed_seen_at": "invalid timestamp"},
])
def test_partial_observation_alone_keeps_one_provisional_match(broken):
    snap = snapshot()
    snap["observations"] = [{**observation(MATCH, START), **broken}]
    packet = prepare(snap, control(), START, END)
    assert packet["fixtures"] == [["9", "2026", MATCH, MATCH]]
    assert f"completed_observation_incomplete:{MATCH}" in packet["issues"]


def test_observations_preserve_conflicting_schedule_and_respect_cutoff():
    snap = snapshot()
    snap["schedules"] = [fixture(MATCH, score=""), fixture(DIRECT)]
    snap["observations"] = [
        observation(MATCH, START), observation(DIRECT, START),
        {**observation(MISSING, START), "first_completed_seen_at": None, "first_completed_raw_key": None},
        {**observation("0badf00d", START), "first_completed_seen_at": (END + timedelta(seconds=1)).isoformat()},
        {**observation("aa111155", START), "kickoff_at": None,
         "first_completed_seen_at": (START - timedelta(days=10)).isoformat()},
    ]
    packet = prepare(snap, control(), START, END)
    assert {r[2] for r in packet["fixtures"]} == {MATCH, DIRECT}
    assert packet["issues"] == [f"completed_observation_conflicts_with_schedule:{MATCH}",
                                "completion_observed_after_cutoff:0badf00d"]


def test_completion_before_corrected_kickoff_remains_one_unverified_match():
    snap = snapshot()
    snap["schedules"] = [fixture(MATCH)]
    snap["observations"] = [{**observation(MATCH, START), "first_completed_seen_at": (START - timedelta(days=3)).isoformat()}]
    packet = prepare(snap, control(), START, END)
    assert packet["fixtures"] == [["9", "2026", MATCH, MATCH]]
    assert packet["issues"] == [f"completion_before_kickoff:{MATCH}"]


def test_scored_fixture_without_parseable_source_date_is_always_preliminary():
    snap = snapshot()
    snap["schedules"] = [fixture(MATCH, date="")]
    packet = prepare(snap, control(), START, END)
    assert packet["fixtures"] == [["9", "2026", MATCH, MATCH]]
    assert "unconfirmed_kickoff" in packet["issues"]


def test_direct_catalog_and_completed_source_observations_have_independent_provenance():
    snap = snapshot()
    snap["schedule_health"] = []
    snap["observations"] = [observation(MATCH, START), observation(DIRECT, START)]
    edition = {"competition_id": "9", "season_id": "2026", "canonical_url": f"/en/matches/{MATCH}",
               "catalog_raw_key": "history", "catalog_source": {"manifest_key": "history"},
               "catalog_match_ids": [MATCH, DIRECT], "targets": [{"match_id": MATCH, "raw_key": "raw"}]}
    packet = prepare(snap, control([edition]), START, END)
    assert len(packet["fixtures"]) == 2 and not packet["issues"]

    # A missing member on the canonical source list is a catalog discrepancy even
    # though both matches have individually confirmed dates and completed reports.
    edition["catalog_match_ids"] = [MATCH]
    assert prepare(snap, control([edition]), START, END)["issues"] == ["direct_catalog_unverified:9/2026"]

    # Without a completed observation or a frontier-backed source receipt, knowing
    # an ID from the accepted catalog still cannot establish its played status.
    edition["catalog_match_ids"] = [MATCH, DIRECT]
    snap["observations"].pop()
    edition["match_proofs"] = [{"match_id": DIRECT, "raw_key": "unverified", "fetched_at": END.isoformat(),
                                "kickoff_at": START.isoformat(), "score": "2-0"}]
    assert prepare(snap, control([edition]), START, END)["issues"] == [f"direct_match_unconfirmed:{DIRECT}"]


def test_direct_member_once_even_when_also_observed_and_unproven_direct_blocks():
    snap = snapshot()
    snap["schedule_health"][0]["fetched"] = False
    snap["observations"] = [observation(MATCH, START)]
    snap["schedules"] = [fixture(MATCH)]
    edition = {"competition_id": "9", "season_id": "2026", "canonical_url": f"/en/matches/{MATCH}",
               "catalog_raw_key": "historyABC", "catalog_source": {"manifest_key": "historyABC"},
               "targets": [{"match_id": MATCH, "raw_key": "one"}, {"match_id": DIRECT, "raw_key": "directXYZ"}],
               "catalog_match_ids": [MATCH, DIRECT],
               "match_proofs": [{"match_id": DIRECT, "kickoff_at": START.isoformat(), "score": "1-1",
                                 "raw_key": "directXYZ", "fetched_at": END.isoformat()}]}
    packet = prepare(snap, control([edition]), START, END)
    assert {r[2] for r in packet["fixtures"]} == {MATCH, DIRECT}
    assert not packet["issues"]
    edition["match_proofs"][0]["fetched_at"] = (END + timedelta(minutes=1)).isoformat()
    edition["catalog_raw_key"] = "other"
    packet = prepare(snap, control([edition]), START, END)
    assert packet["issues"] == ["direct_catalog_unverified:9/2026", f"direct_match_unconfirmed:{DIRECT}"]
    assert len(packet["fixtures"]) == 1


def test_incomplete_source_scope_and_missing_registry_cannot_pass():
    snap = snapshot()
    snap["coverage"]["end"] = "2026-01-14"
    snap["schedule_health"] = []
    snap["errors"].append({"source": "fbref_schedule", "reason": "read_failed"})
    snap["schedules"] = [fixture(MATCH, source_season_id="2025")]
    packet = prepare(snap, {}, START, END)
    assert not packet["fixtures"]
    assert set(packet["issues"]) == {"snapshot_does_not_cover_window", "unverified_cutoff_or_direct_registry",
        "missing_or_unfetched_schedule:9/2026", "snapshot_read_failure_or_unverified",
        "played_edition_outside_current_scope:9/2025"}


@pytest.mark.parametrize("n,empty,mode,verdict", [
    (100, 1, "ok", "PASS"), (99, 1, "ok", "FAIL"), (0, 0, "ok", "PRELIMINARY"),
    (100, 1, "missing_receipt", "PRELIMINARY"), (100, 1, "wrong_batch", "PRELIMINARY"),
    (100, 1, "unknown_scope", "PRELIMINARY"), (100, 1, "wrong_scope", "PRELIMINARY"),
    (100, 1, "unreported", "PRELIMINARY"), (100, 1, "undated", "PRELIMINARY"),
    (100, 1, "duplicate_available", "PRELIMINARY"),
])
def test_threshold_against_physical_rows(n, empty, mode, verdict):
    query = SQL.read_text().replace("__CUTOFF__", "2026-01-15 15:00:00.000000 UTC")
    packet = {"fixtures": [], "pairs": [["9", "2026"]], "issues": ["scope missing"] if mode == "unknown_scope" else []}
    for i in range(n):
        reported = not (i == 0 and mode == "unreported")
        packet["fixtures"].append(["9", "2026", str(i) if reported else None, str(i)])
    query = query.replace("__COHORT_JSON__", json.dumps(packet))
    # Parse the Trino SELECT and adapt only Iceberg time travel and catalog names for local tables.
    assert sqlglot.parse_one(query, read="trino").key == "select"
    query = query.replace(" FOR TIMESTAMP AS OF TIMESTAMP '2026-01-15 15:00:00.000000 UTC'", "")
    query = query.replace("iceberg.bronze.", "")
    with duckdb.connect() as conn:
        conn.execute("CREATE TABLE fbref_schedule(source_competition_id varchar, source_season_id varchar, date varchar, score varchar, notes varchar)")
        if mode == "undated":
            conn.execute("INSERT INTO fbref_schedule VALUES ('9','2026',NULL,'0–0','')")
        conn.execute("CREATE TABLE fbref_lineups(match_id varchar, source_competition_id varchar, source_season_id varchar, _batch_id varchar)")
        conn.execute("CREATE TABLE fbref_dataset_availability(match_id varchar, source_competition_id varchar, source_season_id varchar, dataset varchar, availability varchar, _batch_id varchar)")
        for i in range(n):
            mid = str(i)
            if i or mode != "missing_receipt":
                conn.execute("INSERT INTO fbref_dataset_availability VALUES (?, '9','2026','lineups',?,'batch')",
                             [mid, "restricted" if i < empty else "available"])
            if i == 1 and mode == "duplicate_available":
                conn.execute("INSERT INTO fbref_dataset_availability VALUES (?, '9','2026','lineups','available','another')", [mid])
            if i >= empty:
                conn.execute("INSERT INTO fbref_lineups VALUES (?, '9',?,?)", [
                    mid, "old" if i == 1 and mode == "wrong_scope" else "2026",
                    "other" if i == 1 and mode == "wrong_batch" else "batch"])
        cursor = conn.execute(sqlglot.transpile(query, read="trino", write="duckdb")[0])
        result = dict(zip([c[0] for c in cursor.description], cursor.fetchone()))
    assert result["verdict"] == verdict
    assert result["played"] == n
    assert result["undated_scored_rows"] == (mode == "undated")
    if n:
        assert result["empty_lineups"] == empty + (mode == "wrong_scope")


def test_cli_binds_snapshot_bytes_and_escapes_anonymous_fixture_names(tmp_path, capsys):
    from hashlib import sha256

    from scripts.report_fbref_lineup_coverage import main

    s = snapshot()
    s["schedules"] = [fixture(None, home="Club d'Ouest")]
    source = tmp_path / "snapshot.json"
    source.write_text(json.dumps(s))
    receipt = control()
    receipt["snapshot_sha256"] = sha256(source.read_bytes()).hexdigest()
    direct = tmp_path / "direct.json"
    direct.write_text(json.dumps(receipt))
    args = ["--snapshot", str(source), "--control", str(direct),
            "--deployment-sha", "1" * 40, "--deployed-at", START.isoformat(), "--cutoff", END.isoformat()]
    main(args)
    statement = capsys.readouterr().out
    assert len(sqlglot.parse(statement, read="trino")) == 1
    assert "d''Ouest" in statement and "snapshot_digest_unverified" not in statement
    source.write_text(source.read_text() + "\n")
    main(args)
    assert "snapshot_digest_unverified" in capsys.readouterr().out
