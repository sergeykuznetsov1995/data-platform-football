"""Offline, executable migration regressions; SQLite provides real row mutations."""

import hashlib
import json
import sqlite3

import pytest

from scripts import fotmob_repair_seasons as repair


class Database:
    def __init__(self):
        self.connection = sqlite3.connect(":memory:")
        self.connection.execute("ATTACH DATABASE ':memory:' AS bronze")
        self.connection.execute(
            "CREATE TABLE bronze.fotmob_competition_seasons "
            "(competition_id VARCHAR, source_season_key VARCHAR, "
            "_target_batch_id VARCHAR, _payload_sha256 VARCHAR, _raw_uri VARCHAR, "
            "note VARCHAR, is_latest BOOLEAN)"
        )
        self.connection.execute(
            "CREATE VIEW bronze.fotmob_competition_seasons_current AS "
            "SELECT * FROM fotmob_competition_seasons"
        )
        self.writes = []

    def query(self, sql):
        sql = sql.replace('"iceberg".', "")
        if sql.startswith("DESCRIBE "):
            return [
                (r[1], r[2].lower())
                for r in self.connection.execute(
                    "PRAGMA bronze.table_info('fotmob_competition_seasons')"
                )
            ]
        if sql.startswith(("DELETE ", "INSERT ")):
            self.writes.append(sql)
        return list(self.connection.execute(sql))

    def add(self, season, batch, sha, raw, note="original's value", latest=False):
        self.connection.execute(
            "INSERT INTO bronze.fotmob_competition_seasons VALUES (?,?,?,?,?,?,?)",
            ("337", season, batch, sha, raw, note, latest),
        )

    def rows(self):
        return list(
            self.connection.execute(
                "SELECT * FROM bronze.fotmob_competition_seasons ORDER BY source_season_key"
            )
        )


@pytest.fixture
def fixture(tmp_path):
    payload = {
        "details": {"id": 337, "selectedSeason": "2025 - Apertura"},
        "allAvailableSeasons": ["2025 - Apertura", "2025 - Clausura"],
        "stats": {"seasonsWithLinks": ["2025"]},
    }
    raw = json.dumps(payload)
    path = tmp_path / "catalog.json"
    path.write_text(raw)
    sha = hashlib.sha256(raw.encode()).hexdigest()
    evidence = {"path": str(path), "sha256": sha, "raw_uri": "s3://raw/catalog.json"}
    manifest = {
        "issue": 1231,
        "catalogs": [{"competition_id": 337, **evidence}],
        "targets": [
            {
                "competition_id": 337,
                "source_season_key": "2025",
                "provenance": [{"batch_id": "old-batch", **evidence}],
            }
        ],
    }
    db = Database()
    db.add("2025", "old-batch", sha, evidence["raw_uri"])
    db.add("2025 - Apertura", "new-batch", sha, evidence["raw_uri"])
    db.add("2025 - Clausura", "new-batch", sha, evidence["raw_uri"])
    db.add("2024", "legitimate-batch", "a" * 64, "s3://raw/other")
    return db, manifest, tmp_path


def plan(fixture):
    db, manifest, _ = fixture
    return repair.make_plan(db, manifest, catalog="iceberg", schema="bronze")


def test_dry_run_is_read_only_and_backs_up_complete_rows(fixture):
    db, _, _ = fixture
    before = db.rows()
    result = plan(fixture)
    assert result["rows"][0][-2:] == ["original's value", "0"]
    assert db.rows() == before
    assert db.writes == []


def test_apply_rollback_and_retries_restore_exact_rows(fixture):
    db, _, directory = fixture
    before = db.rows()
    path = directory / "plan.json"
    repair.save_plan(path, plan(fixture))
    assert repair.execute(db, path, mode="apply", writers_quiesced=True) == "applied"
    assert [r[1] for r in db.rows()] == ["2024", "2025 - Apertura", "2025 - Clausura"]
    assert (
        repair.execute(db, path, mode="apply", writers_quiesced=True)
        == "already_applied"
    )
    assert (
        repair.execute(db, path, mode="rollback", writers_quiesced=True) == "restored"
    )
    assert (
        repair.execute(db, path, mode="rollback", writers_quiesced=True)
        == "already_restored"
    )
    assert db.rows() == before


@pytest.mark.parametrize(
    "field,value",
    [("batch_id", "unknown"), ("sha256", "b" * 64), ("raw_uri", "s3://wrong")],
)
def test_wrong_provenance_refused(fixture, field, value):
    db, manifest, _ = fixture
    manifest["targets"][0]["provenance"][0][field] = value
    with pytest.raises(ValueError):
        plan(fixture)
    assert not db.writes


def test_bare_label_in_primary_catalog_is_legitimate(fixture):
    db, manifest, directory = fixture
    raw = json.loads((directory / "catalog.json").read_text())
    raw["allAvailableSeasons"].append("2025")
    text = json.dumps(raw)
    (directory / "catalog.json").write_text(text)
    sha = hashlib.sha256(text.encode()).hexdigest()
    manifest["catalogs"][0]["sha256"] = sha
    manifest["targets"][0]["provenance"][0]["sha256"] = sha
    with pytest.raises(ValueError, match="catalog|phantom"):
        plan(fixture)
    assert not db.writes


def test_additional_unreviewed_batch_refused(fixture):
    db, _, _ = fixture
    db.add("2025", "unreviewed", "b" * 64, "s3://other")
    with pytest.raises(ValueError, match="provenance"):
        plan(fixture)


@pytest.mark.parametrize("mode", ["apply", "rollback"])
def test_changed_rows_abort_before_any_write(fixture, mode):
    db, _, directory = fixture
    path = directory / "plan.json"
    repair.save_plan(path, plan(fixture))
    db.connection.execute(
        "UPDATE bronze.fotmob_competition_seasons SET note='changed' WHERE source_season_key='2025'"
    )
    with pytest.raises(ValueError, match="changed"):
        repair.execute(db, path, mode=mode, writers_quiesced=True)
    assert not db.writes


def test_tampered_backup_and_missing_quiescence_refused(fixture):
    db, _, directory = fixture
    path = directory / "plan.json"
    repair.save_plan(path, plan(fixture))
    with pytest.raises(ValueError, match="quiesc"):
        repair.execute(db, path, mode="apply", writers_quiesced=False)
    data = json.loads(path.read_text())
    data["rows"][0][-2] = "tamper"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="digest"):
        repair.execute(db, path, mode="apply", writers_quiesced=True)
    assert not db.writes


def test_changed_current_evidence_refused(fixture):
    db, _, directory = fixture
    path = directory / "plan.json"
    repair.save_plan(path, plan(fixture))
    db.connection.execute(
        "UPDATE bronze.fotmob_competition_seasons SET _payload_sha256='new' WHERE source_season_key LIKE '% - %'"
    )
    with pytest.raises(ValueError, match="current"):
        repair.execute(db, path, mode="apply", writers_quiesced=True)
    assert not db.writes


def test_plan_never_overwrites_existing_backup(fixture):
    _, _, directory = fixture
    path = directory / "plan.json"
    data = plan(fixture)
    repair.save_plan(path, data)
    with pytest.raises(FileExistsError):
        repair.save_plan(path, data)


def test_partial_delete_refused_and_unknown_competition_refused(fixture):
    _, manifest, _ = fixture
    manifest["targets"][0]["competition_id"] = 999
    with pytest.raises(ValueError):
        plan(fixture)


def test_duplicate_physical_rows_preserved_on_rollback(fixture):
    db, manifest, directory = fixture
    entry = manifest["targets"][0]["provenance"][0]
    db.add("2025", entry["batch_id"], entry["sha256"], entry["raw_uri"])
    before = db.rows()
    path = directory / "plan.json"
    repair.save_plan(path, plan(fixture))
    repair.execute(db, path, mode="apply", writers_quiesced=True)
    repair.execute(db, path, mode="rollback", writers_quiesced=True)
    assert db.rows() == before


def test_partial_population_never_causes_partial_restore(fixture):
    db, manifest, directory = fixture
    entry = manifest["targets"][0]["provenance"][0]
    entry2 = {**entry, "batch_id": "old-batch-2"}
    manifest["targets"][0]["provenance"].append(entry2)
    db.add("2025", entry2["batch_id"], entry2["sha256"], entry2["raw_uri"])
    path = directory / "plan.json"
    repair.save_plan(path, plan(fixture))
    db.connection.execute(
        "DELETE FROM bronze.fotmob_competition_seasons WHERE _target_batch_id='old-batch-2'"
    )
    for mode in ("apply", "rollback"):
        with pytest.raises(ValueError, match="changed"):
            repair.execute(db, path, mode=mode, writers_quiesced=True)
    assert not db.writes


def test_lost_response_retries_do_not_duplicate_rows(fixture):
    db, _, directory = fixture
    path = directory / "plan.json"
    repair.save_plan(path, plan(fixture))
    original = db.query

    def fail_after_commit(sql):
        result = original(sql)
        if sql.startswith(("DELETE ", "INSERT ")):
            raise OSError("response lost after atomic commit")
        return result

    db.query = fail_after_commit
    with pytest.raises(OSError):
        repair.execute(db, path, mode="apply", writers_quiesced=True)
    db.query = original
    assert (
        repair.execute(db, path, mode="apply", writers_quiesced=True)
        == "already_applied"
    )
    db.query = fail_after_commit
    with pytest.raises(OSError):
        repair.execute(db, path, mode="rollback", writers_quiesced=True)
    db.query = original
    assert (
        repair.execute(db, path, mode="rollback", writers_quiesced=True)
        == "already_restored"
    )
    assert len(db.rows()) == 4


def test_unsupported_schema_fails_closed(fixture):
    db, _, _ = fixture
    original = db.query

    def unsupported(sql):
        result = original(sql)
        return (
            result + [("nested", "array(varchar)")]
            if sql.startswith("DESCRIBE ")
            else result
        )

    db.query = unsupported
    with pytest.raises(ValueError, match="unsupported"):
        plan(fixture)
    assert not db.writes


def test_no_network_for_help_or_apply_without_quiescence(monkeypatch, capsys):
    from scripts import fotmob_acceptance

    def unexpected(**kwargs):
        pytest.fail("must validate flags before creating a database connection")

    monkeypatch.setattr(fotmob_acceptance, "connect_from_env", unexpected)
    with pytest.raises(SystemExit) as exc:
        repair.main(["--help"])
    assert exc.value.code == 0
    with pytest.raises(SystemExit) as exc:
        repair.main(["apply", "--plan", "unused.json"])
    assert exc.value.code == 2


def test_rollback_restores_null_and_quoted_cells(fixture):
    db, _, directory = fixture
    db.connection.execute(
        "UPDATE bronze.fotmob_competition_seasons SET note=NULL WHERE source_season_key='2025'"
    )
    before = db.rows()
    path = directory / "plan.json"
    repair.save_plan(path, plan(fixture))
    repair.execute(db, path, mode="apply", writers_quiesced=True)
    repair.execute(db, path, mode="rollback", writers_quiesced=True)
    assert db.rows() == before
