from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re

import pandas as pd
import pytest

from dags.utils import transfermarkt_achievements as achievements
from scrapers.transfermarkt.achievement_observations import (
    ACHIEVEMENT_OBSERVATION_KEY,
    AchievementRecipient,
    AchievementUniverse,
    achievement_url,
    extract_achievement_capture,
)
from tests.unit.utils.test_transfermarkt_current_write import (
    MemoryCursor, MemoryDB, MemoryWriter, PREFLIGHT, scraper,  # noqa: F401
)

pytestmark = pytest.mark.unit
EXTRACTED = datetime(2026, 11, 20, 12, 0, 0, 123456, tzinfo=timezone.utc)
COMMITTED = datetime(2026, 11, 20, 13, 0, 0, 654321, tzinfo=timezone.utc)
CAPTURED = datetime(2026, 11, 20, 10, 0, 0, 167286, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def clean_transfermarkt_environment(monkeypatch):
    for name in (
        "TM_REQUIRE_METERED_PROXY", "TM_PROXY_CONTROL_URL", "TM_DAG_ID",
        "TM_WRITE_MODE", "TM_READER_REVISION",
    ):
        monkeypatch.delenv(name, raising=False)


class AchievementCursor(MemoryCursor):
    def execute(self, sql, params=()):
        params = tuple(
            value.isoformat(timespec="microseconds") if isinstance(value, datetime) else value
            for value in params
        )
        sql = sql.replace(
            "transfermarkt_achievement_receipts_v1",
            "transfermarkt_native_write_manifest_v2",
        )
        target = "'" + achievements.ACHIEVEMENT_TABLE + "'"
        placeholder = "__achievement_target_table__"
        sql = sql.replace(target, "'" + placeholder + "'")
        result = super().execute(sql, params)
        if placeholder in sql:
            self.db.sql.execute(
                "UPDATE transfermarkt_native_write_manifest_v2 SET target_table=? WHERE target_table=?",
                (achievements.ACHIEVEMENT_TABLE, placeholder),
            )
        return result


class AchievementDB(MemoryDB):
    def __init__(self):
        super().__init__()
        self.mutation = None

    def cursor(self):
        return AchievementCursor(self)


class AchievementWriter(MemoryWriter):
    def _write_to_iceberg(self, *args, **kwargs):
        result = super()._write_to_iceberg(*args, **kwargs)
        if self.db.mutation == "extra":
            self.db.sql.execute(
                "INSERT INTO transfermarkt_achievement_observations "
                "SELECT * FROM transfermarkt_achievement_observations LIMIT 1"
            )
        elif self.db.mutation == "missing":
            self.db.sql.execute(
                "DELETE FROM transfermarkt_achievement_observations WHERE rowid = "
                "(SELECT MIN(rowid) FROM transfermarkt_achievement_observations)"
            )
        return result


@pytest.fixture
def achievement_policy(monkeypatch, tmp_path):
    source = Path("dags/configs/transfermarkt/achievements_standing_approval_policy.json")
    path = tmp_path / "achievements-policy.json"
    path.write_text(source.read_text())
    monkeypatch.setenv("TM_ACHIEVEMENTS_POLICY_PATH", str(path))
    return path


@pytest.fixture
def physical(scraper, monkeypatch, tmp_path, achievement_policy):  # noqa: F811
    monkeypatch.setenv("TM_WRITER_LOCK_PATH", str(tmp_path / "writer.lock"))
    monkeypatch.delenv("TM_WRITER_LOCK_SHARED_FILE_ID", raising=False)
    db = AchievementDB()
    original_cell = achievements.current._cell

    def fake_cell(column, value):
        if column in achievements.TIMESTAMP_COLUMNS:
            return achievements._canonical_cell(column, value)
        return original_cell(column, value)

    monkeypatch.setattr(achievements.current, "_cell", fake_cell)
    scraper.test_db = db
    scraper._bronze_connection = lambda: db
    scraper._iceberg_writer = AchievementWriter(db)
    return scraper


def extraction(
    instance,
    *,
    kind="player",
    rows=2,
    at=EXTRACTED,
    captured=CAPTURED,
    cycle="raw-cycle-1",
    run="run-1",
):
    slug = "test-coach" if kind == "coach" else None
    recipient = AchievementRecipient(kind, 7, slug)
    if rows == 0:
        body = b'{"achievements":[]}'
    else:
        detail = {
            "id": 17, "name": "Cup", "trophyId": 31,
            "seasonId": 2022, "season": {"id": 2022, "display": "2022"},
            "details": {"playerId": 7, "clubId": 61, "competitionId": "Q1"},
        }
        body = json.dumps({
            "success": True,
            "data": {
                "aggregated": [{"id": 17, "receivedCount": rows, "name": "Cup"}],
                "achievements": [dict(detail) for _ in range(rows)],
            },
        }).encode()
    raw = instance.test_store.store_attempt(
        url=achievement_url(**recipient.inventory_entry()), body=body, status_code=200,
        headers={"Content-Type": "application/json"}, fetched_at=captured.isoformat(),
        cycle_id=cycle, scope_id=f"achievement:{kind}:7", endpoint=f"{kind}_achievement",
        attempt=0,
    )
    universe = AchievementUniverse("registry-writer-test", (recipient,))
    return extract_achievement_capture(
        instance.test_store.load_capture(raw.capture_id)[0],
        raw, recipient=recipient, universe=universe, run_id=run, extracted_at=at,
    )


def write(instance, result, *, at=COMMITTED):
    return achievements.write_achievement_observations(
        instance, result, PREFLIGHT, committed_at=at,
    )


def business_table(instance):
    return pd.read_sql_query(
        "SELECT * FROM transfermarkt_achievement_observations", instance.test_db.sql,
    )


def receipt_table(instance):
    return pd.read_sql_query(
        "SELECT * FROM transfermarkt_native_write_manifest_v2", instance.test_db.sql,
    )


def test_ddl_has_complete_schema_and_no_delete():
    ddl = achievements.achievement_ddl()
    assert len(ddl) == 2
    assert achievements.ACHIEVEMENT_TABLE in ddl[0]
    assert achievements.ACHIEVEMENT_RECEIPTS_TABLE in ddl[1]
    for column in achievements.ACHIEVEMENT_WRITER_COLUMNS:
        assert re.search(rf"\b{re.escape(column)}\b", ddl[0])
    for column in achievements.RECEIPT_COLUMNS:
        assert re.search(rf"\b{re.escape(column)}\b", ddl[1])
    assert "DELETE" not in "\n".join(ddl)


def test_writer_returns_per_recipient_full_readback_and_receipt(physical):
    result = extraction(physical)
    receipt = write(physical, result)
    assert receipt["verified"] is True and receipt["receipt_readback"] is True
    assert receipt["recipient_type"] == "player" and receipt["recipient_id"] == "7"
    assert receipt["detail_rows"] == 2 and receipt["empty_rows"] == 0
    assert receipt["row_count"] == 2 and receipt["status_counts"] == {"observed_present": 2}
    assert receipt["natural_key_columns"] == list(ACHIEVEMENT_OBSERVATION_KEY)
    assert receipt["source_capture_id"] == result.raw_capture_id
    assert receipt["source_body_hash"] == result.source_body_hash
    assert receipt["committed_batch"] == achievements._batch_id(result)
    assert len(receipt["plan_sha256"]) == len(receipt["physical_hash"]) == 64
    assert receipt["committed_at"] == COMMITTED.isoformat()
    rows = business_table(physical)
    assert len(rows) == 2
    assert list(rows.columns) == list(achievements.ACHIEVEMENT_WRITER_COLUMNS)
    assert not rows.duplicated(list(ACHIEVEMENT_OBSERVATION_KEY)).any()
    assert set(rows["source_row_id"]) == {"achievements:0", "achievements:1"}
    stored = receipt_table(physical).iloc[0]
    assert stored["receipt_id"] == receipt["receipt_id"]
    assert stored["physical_hash"] == receipt["physical_hash"]


def test_identical_replay_is_noop_and_receipt_reread(physical):
    result = extraction(physical)
    first = write(physical, result)
    repeat = write(physical, result)
    assert repeat["receipt_id"] == first["receipt_id"]
    assert len(business_table(physical)) == 2
    assert len(receipt_table(physical)) == 1


def test_new_capture_appends_history_without_overwriting(physical):
    first_result = extraction(physical)
    first = write(physical, first_result)
    later_result = extraction(
        physical, at=EXTRACTED + timedelta(hours=1), captured=CAPTURED + timedelta(hours=1),
        cycle="raw-cycle-2", run="run-2",
    )
    later = write(physical, later_result, at=COMMITTED + timedelta(hours=1))
    assert later["receipt_id"] != first["receipt_id"]
    assert len(business_table(physical)) == 4
    assert len(receipt_table(physical)) == 2
    assert set(business_table(physical)["source_capture_id"]) == {
        first_result.raw_capture_id, later_result.raw_capture_id,
    }


def test_conclusive_empty_has_one_status_row_and_receipt(physical):
    result = extraction(physical, rows=0)
    receipt = write(physical, result)
    rows = business_table(physical)
    assert len(rows) == 1
    assert rows.iloc[0]["observation_status"] == "observed_empty"
    assert rows.iloc[0]["source_row_id"] == "recipient"
    assert rows.iloc[0]["title"] is None
    assert receipt["detail_rows"] == 0 and receipt["empty_rows"] == 1


def test_missing_receipt_is_recovered_without_second_business_write(physical):
    result = extraction(physical)
    first = write(physical, result)
    physical.test_db.sql.execute(
        "DELETE FROM transfermarkt_native_write_manifest_v2 WHERE receipt_id=?",
        (first["receipt_id"],),
    )
    recovered = write(physical, result)
    assert recovered["receipt_id"] == first["receipt_id"]
    assert len(business_table(physical)) == 2
    assert len(receipt_table(physical)) == 1


@pytest.mark.parametrize("mutation", ["extra", "missing"])
def test_physical_loss_or_extra_row_fails(physical, mutation):
    physical.test_db.mutation = mutation
    with pytest.raises(Exception, match="readback|read-back|differs|missing|extra|mismatch"):
        write(physical, extraction(physical))


def test_same_capture_with_changed_extraction_is_rejected(physical):
    result = extraction(physical)
    write(physical, result)
    changed = extraction(physical, at=EXTRACTED + timedelta(hours=1))
    with pytest.raises(Exception, match="differs|immutable|plan"):
        write(physical, changed)
    assert len(business_table(physical)) == 2


def test_policy_denial_happens_before_business_write(physical, achievement_policy):
    policy = json.loads(achievement_policy.read_text())
    policy["allowed_write_tables"].remove(achievements.ACHIEVEMENT_TABLE)
    achievement_policy.write_text(json.dumps(policy))
    with pytest.raises(Exception, match="does not authorize"):
        write(physical, extraction(physical))
    exists = physical.test_db.sql.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        ("transfermarkt_achievement_observations",),
    ).fetchone()[0]
    assert exists == 0
