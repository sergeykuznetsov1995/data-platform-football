from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

import pytest

from dags.utils.transfermarkt_approval import ApprovalJournal


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "prepare_transfermarkt_scope_approvals.py"
PARENT_CYCLE_ID = "manual__tm_ingest_20260712T200000Z"
SNAPSHOT_ID = "tm-discovery-0123456789abcdef01234567"


def _load():
    spec = importlib.util.spec_from_file_location("tm_scope_approvals_cli", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class _Cursor:
    """Serves the promoted registry read and the reader-state read."""

    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.description = None
        self._result: list[tuple] = []

    def execute(self, query: str, *args, **kwargs):
        if "transfermarkt_reader_state_v2" in query:
            self.description = [
                ("active_version",), ("active_slot",), ("revision",),
            ]
            self._result = []
            return
        columns = list(self.rows[0]) if self.rows else list(_registry_row("X", "1"))
        self.description = [(name,) for name in columns]
        self._result = [tuple(row.values()) for row in self.rows]

    def fetchall(self):
        return self._result

    def fetchone(self):
        return self._result[0] if self._result else None


def _registry_row(competition_id: str, edition_id: str) -> dict:
    return {
        "competition_id": competition_id,
        "slug": "premier-league",
        "name": "Premier League",
        "country": "England",
        "confederation": "UEFA",
        "competition_type": "domestic_league",
        "gender": "men",
        "team_type": "club",
        "age_category": "senior",
        "competition_season_format": "split_year",
        "competition_active": True,
        "competition_source_url": "https://www.transfermarkt.com/x",
        "competition_discovered_at": datetime(2026, 7, 12, tzinfo=timezone.utc),
        "canonical_competition_id": competition_id,
        "classification_status": "eligible",
        "classification_evidence": [
            {
                "source_field": "section",
                "source_value": "National Leagues",
                "source_url": "https://www.transfermarkt.com/x",
                "origin": "structured",
                "signals": {
                    "competition_type": "domestic_league",
                    "gender": "men",
                    "team_type": "club",
                    "age_category": "senior",
                    "season_format": "split_year",
                },
            }
        ],
        "competition_source_body_hash": "a" * 64,
        "competition_parser_revision": 1,
        "competition_schema_revision": 1,
        "edition_id": edition_id,
        "edition_label": "2025/26",
        "canonical_season": "2526",
        "edition_season_format": "split_year",
        "start_date": None,
        "end_date": None,
        "edition_active": True,
        "is_current": True,
        "participant_count": 20,
        "participant_hash": "b" * 64,
        "edition_source_url": "https://www.transfermarkt.com/x/2025",
        "edition_discovered_at": datetime(2026, 7, 12, tzinfo=timezone.utc),
        "edition_source_body_hash": "c" * 64,
        "edition_parser_revision": 1,
        "edition_schema_revision": 1,
        "registry_snapshot_id": SNAPSHOT_ID,
        "last_success_at": None,
    }


def _plan(
    module,
    tmp_path: Path,
    rows: list[dict] | None = None,
    parent_cycle_id: str = PARENT_CYCLE_ID,
):
    return module.build_plan(
        parent_cycle_id,
        registry_snapshot_id=SNAPSHOT_ID,
        approval_root=tmp_path / "approvals",
        journal_path=tmp_path / "approvals" / "journal.json",
        cursor=_Cursor(
            [_registry_row("GB1", "2025")] if rows is None else rows
        ),
    )


def test_the_same_scope_can_be_crawled_again_by_a_later_cycle(tmp_path):
    module = _load()
    first = _plan(module, tmp_path)
    module.apply_plan(first)

    # A one-shot packet id is never reusable and its file is never overwritten,
    # so a refresh of the same scope needs packets of its own.
    second = _plan(module, tmp_path, parent_cycle_id="manual__tm_ingest_later")
    scope_id = second["scope_ids"][0]
    assert scope_id == first["scope_ids"][0]
    for kind in ("paid", "write"):
        assert (
            second["packets"][scope_id][kind].packet_id
            != first["packets"][scope_id][kind].packet_id
        )
    module.apply_plan(second)


def test_paid_and_write_packets_share_the_childs_own_argv(tmp_path):
    module = _load()
    plan = _plan(module, tmp_path)

    assert plan["scope_count"] == 1
    scope_id = plan["scope_ids"][0]
    bundle = plan["packets"][scope_id]
    paid, write = bundle["paid"], bundle["write"]

    assert tuple(paid.argv) == tuple(write.argv)
    assert paid.argv[1] == module.CHILD_SCRIPT
    # The approval flags must never appear: they reference the packets themselves.
    assert not [item for item in paid.argv if "approval-packet" in item]
    assert paid.packet_hash != write.packet_hash


def test_packets_carry_the_payload_the_dag_will_rebuild_from_the_conf(tmp_path):
    module = _load()
    rows = [_registry_row("GB1", "2025"), _registry_row("ES1", "2025")]
    plan = _plan(module, tmp_path, rows=rows)

    # The DAG re-plans from the exact scopes in the conf, so the payload it hands
    # the child states a fully covered batch — no due remainder, no continuation.
    argv = plan["packets"][plan["scope_ids"][0]]["paid"].argv
    payload = json.loads(argv[argv.index("--payload-json") + 1])
    assert payload["remaining_count"] == 0
    assert payload["continuation_required"] is False


def test_packet_limits_match_what_the_child_verifies(tmp_path):
    module = _load()
    plan = _plan(module, tmp_path)
    bundle = plan["packets"][plan["scope_ids"][0]]
    paid, write = bundle["paid"], bundle["write"]
    required = {
        value.split(".")[-1] for value in module.required_write_tables(plan["write_mode"])
    }

    assert paid.byte_cap_bytes == module.PROVIDER_HARD_CAP_BYTES
    assert paid.request_limit == module.PROXY_REQUEST_LIMIT
    assert paid.retry_limit == module.PROXY_RETRY_LIMIT
    assert paid.concurrency == 1
    assert write.byte_cap_bytes == 0
    assert write.request_limit == 0
    assert write.concurrency == 1
    for packet in (paid, write):
        approved = {str(value).split(".")[-1] for value in packet.affected_tables}
        assert required.issubset(approved)


def test_apply_approves_every_packet_and_emits_the_trigger_conf(tmp_path):
    module = _load()
    plan = _plan(module, tmp_path)
    now = datetime(2026, 7, 12, 20, 0, tzinfo=timezone.utc)

    result = module.apply_plan(plan, now=now)

    assert result["status"] == "approved"
    journal = ApprovalJournal(plan["journal_path"], clock=lambda: now)
    scope_id = plan["scope_ids"][0]
    for key in ("paid", "write"):
        packet = plan["packets"][scope_id][key]
        assert journal.get(packet.packet_hash).status == "approved"

    index = result["trigger_argv"].index("--conf")
    conf = json.loads(result["trigger_argv"][index + 1])
    bundle = conf["approval_bundles"][scope_id]
    assert len(set(bundle.values())) == 4
    assert conf["registry_snapshot_id"] == SNAPSHOT_ID


def test_a_registry_without_due_scopes_is_refused(tmp_path):
    module = _load()

    with pytest.raises(Exception, match="no scopes|no due scope"):
        _plan(module, tmp_path, rows=[])
