import importlib.util
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "migrate_fotmob_native.py"
SPEC = importlib.util.spec_from_file_location("migrate_fotmob_native", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

QUARANTINE_REASON = MODULE.QUARANTINE_REASON
QuarantineSpec = MODULE.QuarantineSpec
apply_quarantine = MODULE.apply_quarantine
duplicate_rank_sql = MODULE.duplicate_rank_sql


def test_duplicate_query_ranks_source_identity_across_false_seasons():
    spec = QuarantineSpec("fotmob_player_details", ("player_id",))
    sql = duplicate_rank_sql(spec)
    assert 'PARTITION BY "player_id"' in sql
    assert "ORDER BY _ingested_at DESC, season DESC" in sql
    assert "duplicate_rank > 1" in sql


class FakeTrino:
    def __init__(self):
        self.executed = []
        self.query_results = [[(2,)], [(2,)], [(0,)]]

    def get_table_columns(self, schema, table):
        return [
            "player_id",
            "season",
            "name",
            "_ingested_at",
            "_batch_id",
        ]

    def _execute(self, sql):
        self.executed.append(sql)

    def execute_query(self, sql):
        return self.query_results.pop(0)


def test_apply_copies_validates_then_deletes_exact_identity():
    trino = FakeTrino()
    result = apply_quarantine(
        trino, QuarantineSpec("fotmob_player_details", ("player_id",))
    )
    sql = "\n".join(trino.executed)
    assert result["quarantined"] == 2
    assert result["remaining_duplicates"] == 0
    assert sql.index("INSERT INTO") < sql.index("DELETE FROM")
    assert QUARANTINE_REASON in sql
    assert 'q."player_id" IS NOT DISTINCT FROM source."player_id"' in sql
    assert "q._batch_id IS NOT DISTINCT FROM source._batch_id" in sql
