"""Publication-fence guardrails for every downstream Understat Bronze reader."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]

SQL_READERS = (
    PROJECT_ROOT / "dags/sql/silver/understat_player_season_aggregate.sql",
    PROJECT_ROOT / "dags/sql/silver/understat_player_match_aggregate.sql",
    PROJECT_ROOT / "dags/sql/silver/understat_shots.sql",
    PROJECT_ROOT / "dags/sql/silver/understat_team_match.sql",
    PROJECT_ROOT / "dags/sql/silver/xref_match.sql",
    PROJECT_ROOT / "dags/sql/silver/xref_team.sql.j2",
    PROJECT_ROOT / "dags/sql/gold/fct_shot.sql",
)

TOP_FIVE_SCHEDULE_CONSUMERS = (
    PROJECT_ROOT / "dags/sql/silver/xref_match.sql",
    PROJECT_ROOT / "dags/sql/silver/xref_team.sql.j2",
    PROJECT_ROOT / "dags/sql/gold/fct_shot.sql",
)

pytestmark = pytest.mark.unit


def _body(path: Path) -> str:
    return "\n".join(
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("--")
    )


@pytest.mark.parametrize("path", SQL_READERS, ids=lambda path: path.name)
def test_understat_reader_uses_latest_v2_manifest_attempt(path: Path):
    sql = _body(path)

    assert "iceberg.ops.understat_ingest_manifest_v1" in sql
    assert "contract_version = 'understat-bronze-v2'" in sql
    assert re.search(
        r"ROW_NUMBER\s*\(\s*\)\s+OVER\s*\(\s*"
        r"PARTITION\s+BY\s+league\s*,\s*season\s+"
        r"ORDER\s+BY\s+completed_at\s+DESC\s*,\s*attempt_id\s+DESC",
        sql,
        re.IGNORECASE,
    ), f"{path.name} must choose the actual latest scope attempt"
    assert re.search(r"WHERE\s+rn\s*=\s*1", sql, re.IGNORECASE)


@pytest.mark.parametrize("path", SQL_READERS, ids=lambda path: path.name)
def test_understat_reader_allows_only_legacy_or_exact_complete_batch(path: Path):
    sql = _body(path)

    assert re.search(r"m\.league\s+IS\s+NULL", sql, re.IGNORECASE), (
        f"{path.name} must preserve scopes that predate the manifest"
    )
    assert re.search(r"m\.status\s*=\s*'complete'", sql, re.IGNORECASE), (
        f"{path.name} must fail closed for pending/failed latest attempts"
    )
    assert re.search(
        r"\w+\._batch_id\s*=\s*m\.batch_id", sql, re.IGNORECASE
    ), f"{path.name} must read only the batch certified by the manifest"


@pytest.mark.parametrize(
    "path", TOP_FIVE_SCHEDULE_CONSUMERS, ids=lambda path: path.name
)
def test_cross_source_schedule_consumers_explicitly_exclude_rfpl(path: Path):
    sql = _body(path)

    for league in (
        "ENG-Premier League",
        "ESP-La Liga",
        "GER-Bundesliga",
        "ITA-Serie A",
        "FRA-Ligue 1",
    ):
        assert league in sql
    assert "RUS-Premier League" not in sql


def test_score_crossvalidation_uses_the_same_understat_publication_fence():
    script = _body(PROJECT_ROOT / "scripts/crossvalidate_fbref_scores.py")

    assert "iceberg.ops.understat_ingest_manifest_v1" in script
    assert "contract_version = 'understat-bronze-v2'" in script
    assert "m.status = 'complete'" in script
    assert "s._batch_id = m.batch_id" in script


def test_helper_bronze_reads_repeat_the_top_five_boundary():
    player_match = _body(
        PROJECT_ROOT
        / "dags/sql/silver/understat_player_match_aggregate.sql"
    )
    shots = _body(PROJECT_ROOT / "dags/sql/silver/understat_shots.sql")

    assert "b.league IN (" in player_match
    assert "s.league IN (" in player_match
    assert "s.league IN (" in shots
    assert "p.league IN (" in shots
    assert "RUS-Premier League" not in player_match
    assert "RUS-Premier League" not in shots
