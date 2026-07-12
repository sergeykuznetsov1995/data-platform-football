"""Transfermarkt native-v2 contracts used by the league dashboard."""

from __future__ import annotations

from configs.superset.dashboards import league_overview


def _virtual_sql(monkeypatch) -> dict[str, str]:
    captured: dict[str, str] = {}

    def fake_ensure(_ctx, _database, _schema, name, sql, labels=None):
        del labels
        captured[name] = sql
        return name

    monkeypatch.setattr(
        league_overview, "_ensure_virtual_dataset", fake_ensure,
    )
    league_overview._build_virtual_datasets(object(), object())
    return captured


def test_transfer_dataset_uses_native_event_contract(monkeypatch):
    sql = _virtual_sql(monkeypatch)["v_lo_transfer"]

    assert "t.transfer_id" in sql
    assert "t.event_season AS season" in sql
    assert "    t.league," not in sql
    assert "MONTH(t.transfer_date)" not in sql
    assert "iceberg.silver.transfermarkt_players" in sql
    assert "transfermarkt_player_attributes_v2" not in sql


def test_transfer_league_is_inferred_from_destination_team_season(monkeypatch):
    sql = _virtual_sql(monkeypatch)["v_lo_transfer"]

    assert "SELECT DISTINCT team_id, league, season" in sql
    assert "st.team_id = b.to_team_id" in sql
    assert "st.season  = b.season" in sql
    assert "st.league  = b.league" not in sql
    assert "(st.team_id IS NOT NULL) AS is_incoming" in sql


def test_player_contract_uses_stable_canonical_reader(monkeypatch):
    sql = _virtual_sql(monkeypatch)["v_lo_player_season"]

    assert "iceberg.silver.transfermarkt_players" in sql
    assert "transfermarkt_player_attributes_v2" not in sql
    assert "canonical_id, league, season, contract_until" in sql
