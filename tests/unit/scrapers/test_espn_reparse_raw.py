"""Offline replay: exact blobs, no source transport, safe repeated replacement."""

import csv
import gzip
import hashlib
import json
from pathlib import Path
import re
import socket

import pandas as pd
import pytest

from scrapers.espn import reparse_raw as replay
from scrapers.espn.bronze_schema import MATCH_TABLE, NATURAL_KEYS
from scrapers.espn.raw_store import EspnRawStore
from tests.unit.scrapers.test_espn_bronze_writer import FakeTrino, _SCOPE
from tests.unit.scrapers.test_espn_probes import _bytes

pytestmark = pytest.mark.unit


class Bronze(FakeTrino):
    fail_match = False

    def execute_query(self, sql):
        fields, table, scope = re.match(
            r"SELECT (.*?) FROM iceberg.bronze.(\w+) WHERE (.*)", sql
        ).groups()
        match = _SCOPE.match(scope)
        ids = {int(x) for x in match["ids"].split(", ")}
        return [
            tuple(None if pd.isna(row[f]) else row[f] for f in fields.split(", "))
            for row in self.tables[table]
            if row["event_id"] in ids
            and row["competition_slug"] == match["slug"]
            and row["season_year"] == int(match["year"])
        ]

    def insert_dataframe_atomic(self, schema, table, df, **kwargs):
        if self.fail_match and table == MATCH_TABLE:
            self.fail_match = False
            raise RuntimeError("interrupted before match")
        return super().insert_dataframe_atomic(schema, table, df, **kwargs)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("network/transport forbidden")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    from scrapers.espn.transport import EspnHttpClient

    monkeypatch.setattr(EspnHttpClient, "__init__", forbidden)
    from scrapers.espn.gate import TransportGate
    from scrapers.espn.pace_store import PaceStore

    monkeypatch.setattr(TransportGate, "__init__", forbidden)
    monkeypatch.setattr(PaceStore, "__init__", forbidden)


def corpus(tmp_path):
    data = json.loads(_bytes("summary_jpn1_2026_ten_starters.json"))
    header = data["header"]
    year = header["season"]["year"]
    data["keyEvents"] = [{"id": "k1", "text": "Goal"}]
    data["commentary"] = [{"id": "c1", "sequence": 1, "text": "Comment"}]
    data.setdefault("gameInfo", {})["officials"] = [
        {"displayName": "Referee", "order": 1}
    ]
    for side in header["competitions"][0]["competitors"]:
        side["linescores"] = [{"value": 0}, {"value": 1}]
    for roster in data["rosters"]:
        roster["formation"] = "4-4-2"
    editions = tmp_path / "editions.json"
    editions.write_text(
        json.dumps(
            {
                "version": 1,
                "refreshed_at": "2026-10-26T00:00:00+00:00",
                "editions": [
                    {
                        "slug": header["league"]["slug"],
                        "year": year,
                        "display_name": "Season",
                        "start": f"{year}-01-01",
                        "end": f"{year + 1}-12-31",
                        "open": False,
                    }
                ],
            }
        )
    )
    store = EspnRawStore.from_uri((tmp_path / "raw").as_uri())
    manifest = tmp_path / "ledger.csv"

    def add(payload, fetched="2026-08-02 00:00:00"):
        body = json.dumps(payload).encode()
        sha = hashlib.sha256(body).hexdigest()
        path = Path(store.root) / f"blobs/sha256/{sha[:2]}/{sha}.json.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(gzip.compress(body))
        cid = header["league"]["id"]
        with manifest.open("a") as handle:
            csv.writer(handle).writerow(
                [
                    f"{cid}:{year}",
                    cid,
                    year,
                    header["id"],
                    path.as_uri(),
                    sha,
                    fetched,
                    "old-generation",
                    "old-parser",
                ]
            )
        return path, sha

    add(data)
    return data, manifest, editions, store, add


def test_exact_replay_and_idempotent_write(tmp_path, monkeypatch):
    data, manifest, editions, store, add = corpus(tmp_path)
    older = json.loads(json.dumps(data))
    older.pop("rosters")
    add(older, "2026-08-01 00:00:00")
    # An alias is irrelevant, even when it is unreadable.
    aliases = Path(store.root) / "targets"
    aliases.mkdir()
    (aliases / "alias.json").write_text("different body")
    before = {p: p.read_bytes() for p in Path(store.root).rglob("*") if p.is_file()}

    def forbidden(*args, **kwargs):
        raise AssertionError("writer forbidden in dry-run")

    with monkeypatch.context() as m:
        m.setattr(replay, "write_tournament_batch", forbidden)
        report = replay.run(manifest, editions, store=store)
    assert before == {
        p: p.read_bytes() for p in Path(store.root).rglob("*") if p.is_file()
    }
    record = report["matches"][0]
    assert record["other_variants"] == 1
    assert record["disposition"] == "lineup_anomaly"
    assert all(
        record["parsed"][p]
        for p in (
            "lineup",
            "formations",
            "key_events",
            "commentary",
            "referee",
            "linescores",
        )
    )
    assert report["corpus"]["raw_lineup_event_ids"] == [int(data["header"]["id"])]
    bronze = Bronze()
    for _ in range(2):
        result = replay.run(manifest, editions, store=store, trino=bronze, batch_size=1)
        assert result["matches"][0]["outcome"] == "written", result
    for table, rows in bronze.tables.items():
        keys = [tuple(r[k] for k in NATURAL_KEYS[table]) for r in rows]
        assert len(keys) == len(set(keys))
    assert len(bronze.tables["espn_match_events"]) == 2
    assert bronze.tables[MATCH_TABLE][0]["raw_sha256"] == record["selected_sha256"]


@pytest.mark.parametrize("loss", ["commentary", "referee", "formation", "linescores"])
def test_actual_bronze_loss_is_rejected(tmp_path, loss):
    data, manifest, editions, store, add = corpus(tmp_path)
    bronze = Bronze()
    replay.run(manifest, editions, store=store, trino=bronze)
    if loss == "commentary":
        data["commentary"] = []
        data["keyEvents"].append({"id": "k2", "text": "Same total events"})
    elif loss == "referee":
        data["gameInfo"]["officials"] = []
    elif loss == "formation":
        for roster in data["rosters"]:
            roster.pop("formation")
    else:
        for side in data["header"]["competitions"][0]["competitors"]:
            side.pop("linescores")
    add(data, "2026-08-13 00:00:00")
    calls = len(bronze.calls)
    report = replay.run(manifest, editions, store=store, trino=bronze)
    assert report["matches"][0]["outcome"] == "skipped"
    assert "downgrade_rejected" in report["matches"][0]["write_error"]
    assert len(bronze.calls) == calls


def test_resume_after_child_commit(tmp_path):
    _, manifest, editions, store, _ = corpus(tmp_path)
    bronze = Bronze()
    bronze.fail_match = True
    with pytest.raises(RuntimeError, match="interrupted"):
        replay.run(manifest, editions, store=store, trino=bronze)
    assert not bronze.tables[MATCH_TABLE]
    result = replay.run(manifest, editions, store=store, trino=bronze)
    assert result["matches"][0]["outcome"] == "written"
    assert len(bronze.tables[MATCH_TABLE]) == 1


@pytest.mark.parametrize("broken", ["missing", "gzip", "hash"])
@pytest.mark.parametrize("write", [False, True])
def test_bad_blob_never_downloads(tmp_path, broken, write):
    data, manifest, editions, store, add = corpus(tmp_path)
    path, _ = add(data)
    if broken == "missing":
        path.unlink()
    else:
        path.write_bytes(b"broken" if broken == "gzip" else gzip.compress(b"{}"))
    bronze = Bronze()
    report = replay.run(
        manifest, editions, store=store, trino=bronze if write else None
    )
    assert report["matches"][0]["outcome"] == "no_valid_summary"
    assert report["matches"][0]["errors"]
    assert not bronze.calls


def test_latest_valid_and_scope_identity(tmp_path):
    data, manifest, editions, store, add = corpus(tmp_path)
    bad = json.loads(json.dumps(data))
    bad["rosters"] = "malformed"
    add(bad, "2026-08-13 00:00:00")
    report = replay.run(manifest, editions, store=store)
    record = report["matches"][0]
    assert record["outcome"] == "parsed"
    assert record["errors"][0]["disposition"] == "source_malformed"
    assert report["unique_blobs"] == 2
    wrong = json.loads(json.dumps(data))
    wrong["header"]["id"] = "999999999"
    add(wrong, "2026-08-14 00:00:00")
    record = replay.run(manifest, editions, store=store)["matches"][0]
    assert "identity mismatch" in record["errors"][0]["reason"]
    assert record["outcome"] == "parsed"


def test_preserve_current_schedule_and_publication(tmp_path):
    _, manifest, editions, store, _ = corpus(tmp_path)
    bronze = Bronze()
    replay.run(manifest, editions, store=store, trino=bronze)
    row = bronze.tables[MATCH_TABLE][0]
    row.update(
        home_score=9,
        duplicate_of="owner:2026",
        rechecked_at=row["kickoff"],
        status_checked_at=row["kickoff"],
    )
    expected = {
        name: row[name]
        for name in (
            "home_score",
            "duplicate_of",
            "kickoff",
            "first_published_at",
            "rechecked_at",
            "status_checked_at",
        )
    }
    result = replay.run(manifest, editions, store=store, trino=bronze)
    assert result["matches"][0]["outcome"] == "written", result
    assert {name: bronze.tables[MATCH_TABLE][0][name] for name in expected} == expected


def test_actual_cells_guard_is_independent_of_old_parse(tmp_path):
    _, manifest, editions, store, _ = corpus(tmp_path)
    bronze = Bronze()
    replay.run(manifest, editions, store=store, trino=bronze)
    # Fact exists in Bronze even though reparsing the stored raw cannot see it.
    bronze.tables[MATCH_TABLE][0]["home_score_et"] = 3
    result = replay.run(manifest, editions, store=store, trino=bronze)
    assert result["matches"][0]["outcome"] == "skipped"
    assert "actual Bronze cells" in result["matches"][0]["write_error"]


def test_cli_dry_run_report_and_outside_registry(tmp_path, monkeypatch, capsys):
    _, manifest, editions, store, _ = corpus(tmp_path)
    monkeypatch.setattr(replay.EspnRawStore, "from_env", lambda: store)
    report_path = tmp_path / "report.json"
    replay.main(
        [
            "--manifest",
            str(manifest),
            "--editions",
            str(editions),
            "--dry-run",
            "--report",
            str(report_path),
        ]
    )
    assert json.loads(capsys.readouterr().out) == json.loads(report_path.read_text())
    with manifest.open("a") as handle:
        csv.writer(handle).writerow(
            [
                "99999:2026",
                99999,
                2026,
                123,
                "bad",
                "bad",
                "2026-08-02 00:00:00",
                "old",
                "old",
            ]
        )
    report = replay.run(manifest, editions, store=store)
    assert report["matches"][-1]["outcome"] == "outside_registry"


def test_archive_without_git_uses_release_sha(tmp_path, monkeypatch, capsys):
    _, manifest, editions, store, _ = corpus(tmp_path)
    monkeypatch.setattr(replay.EspnRawStore, "from_env", lambda: store)
    monkeypatch.setenv("ESPN_DEPLOY_GIT_SHA", "a" * 40)

    def forbidden(*args, **kwargs):
        raise AssertionError("git unavailable in release archive")

    monkeypatch.setattr(replay.subprocess, "check_output", forbidden)
    replay.main(["--manifest", str(manifest), "--editions", str(editions), "--dry-run"])
    assert json.loads(capsys.readouterr().out)["code_sha"] == "a" * 40


def test_shares_include_unparseable_matches():
    report = replay._aggregate(
        [
            {
                "event_id": 1,
                "outcome": "parsed",
                "selected_sha256": "a",
                "raw_any": dict.fromkeys(replay.PARTS, True),
                "raw": dict.fromkeys(replay.PARTS, True),
                "parsed": dict.fromkeys(replay.PARTS, True),
            },
            {
                "event_id": 2,
                "outcome": "no_valid_summary",
                "raw_any": dict.fromkeys(replay.PARTS, True),
            },
        ]
    )
    assert report["comparison_matches"] == 2
    assert report["parts"]["lineup"]["raw_share"] == 1
    assert report["parts"]["lineup"]["parsed_share"] == 0.5


def test_missing_edition_remains_in_raw_denominator(tmp_path):
    _, manifest, editions, store, _ = corpus(tmp_path)
    snapshot = json.loads(editions.read_text())
    snapshot["editions"] = []
    editions.write_text(json.dumps(snapshot))
    bronze = Bronze()
    report = replay.run(manifest, editions, store=store, trino=bronze)
    assert report["matches"][0]["outcome"] == "missing_edition"
    assert report["corpus"]["comparison_matches"] == 1
    assert report["corpus"]["parts"]["lineup"]["raw_share"] == 1
    assert report["corpus"]["parts"]["lineup"]["parsed_share"] == 0
    assert not bronze.calls


def test_raw_presence_survives_malformed_neighbour(tmp_path):
    data, manifest, editions, store, add = corpus(tmp_path)
    data["gameInfo"]["officials"] = "malformed"
    add(data, "2026-08-13 00:00:00")
    record = replay.run(manifest, editions, store=store)["matches"][0]
    assert record["raw_any"]["lineup"]
    assert record["raw_any"]["commentary"]
    assert record["errors"][0]["disposition"] == "source_malformed"
