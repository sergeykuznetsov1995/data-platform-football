"""Unit contracts for E3 historical Bronze prechecks."""

from __future__ import annotations

import importlib
import re
import sys


def _load_module():
    sys.modules.pop("dag_e3_backfill", None)
    sys.modules.pop("dags.dag_e3_backfill", None)
    return importlib.import_module("dag_e3_backfill")


def _compact(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def test_understat_precheck_uses_latest_exact_publication_batch():
    module = _load_module()

    sql = _compact(
        module._bronze_scope_count_sql(
            module._UNDERSTAT_SHOTS_TABLE,
            season_sql="2526",
            league_sql="ENG-Premier League",
        )
    )

    assert "iceberg.ops.understat_ingest_manifest_v1" in sql
    assert "contract_version = 'understat-bronze-v2'" in sql
    assert "PARTITION BY league, season" in sql
    assert "ORDER BY completed_at DESC, attempt_id DESC" in sql
    assert "WHERE rn = 1" in sql
    assert "m.status = 'complete'" in sql
    assert "s._batch_id = m.batch_id" in sql


def test_understat_precheck_preserves_only_pre_manifest_legacy_scope():
    module = _load_module()

    sql = _compact(
        module._bronze_scope_count_sql(
            module._UNDERSTAT_SHOTS_TABLE,
            season_sql="1415",
            league_sql="ENG-Premier League",
        )
    )

    assert "LEFT JOIN understat_manifest_latest m" in sql
    assert re.search(
        r"m\.league IS NULL OR \(m\.status = 'complete' "
        r"AND s\._batch_id = m\.batch_id\)",
        sql,
    )


def test_non_understat_precheck_keeps_plain_physical_scope_count():
    module = _load_module()

    sql = module._bronze_scope_count_sql(
        "iceberg.bronze.espn_lineup",
        season_sql="2526",
        league_sql="ENG-Premier League",
    )

    assert sql == (
        "SELECT COUNT(*) FROM iceberg.bronze.espn_lineup "
        "WHERE season = '2526' AND league = 'ENG-Premier League'"
    )
    assert "understat_ingest_manifest" not in sql


def test_espn_precheck_uses_current_view_and_rendered_platform_scope():
    module = _load_module()

    sql = _compact(
        module._bronze_scope_count_sql(
            module._ESPN_LINEUP_TABLE,
            season_sql="2627",
            league_sql="ENG-Premier League",
        )
    )

    assert "iceberg.bronze.espn_lineup_current es_source" in sql
    assert "('700:2026', 700, 2026, 'ENG-Premier League', '2627'" in sql
    assert "es_source.scope_id = espn_scope.scope_id" in sql
    assert "es_source.scope_id IS NULL" in sql
    assert "espn_scope.platform_season_slug = '2627'" in sql
    assert "espn_scope.platform_league = 'ENG-Premier League'" in sql
    assert "__ESPN_DOWNSTREAM" not in sql
