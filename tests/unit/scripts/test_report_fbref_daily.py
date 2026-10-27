import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from scripts.report_fbref_daily import host_postgres, host_trino, main

ROOT = Path(__file__).resolve().parents[3]


def marked_csv(query, payload=""):
    widths = {"fbref_table_inventory": 2, "fbref_schedule": 9,
              "fotmob_competition_seasons": 4, "fotmob_ingest_manifest": 4,
              "fotmob_matches_current": 9}
    width = next(width for table, width in widths.items() if table in query)
    return "".join('"fbref_report_data",' + row + "\n" for row in payload.splitlines()) + (
        ",".join(['"fbref_report_end"'] * (width + 1)) + "\n")


def test_cli_writes_dated_auditable_provisional_report(tmp_path, capsys):
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps({"competitions": [], "seasons": [], "schedules": [], "errors": ["not delivered"]}))
    result = main(["--input", str(input_path), "--date", "2026-10-07", "--as-of", "2026-10-08T07:00:00Z",
                   "--lookback", "3", "--output-dir", str(tmp_path / "out"), "--require-final"])
    assert result == 2
    assert "предварительный" in capsys.readouterr().out
    files = list((tmp_path / "out").glob("*.json"))
    assert len(files) == 2
    bundle = json.loads(next(p for p in files if "snapshot" not in p.name).read_text())
    assert bundle["milestone_streak"] == 0 and bundle["latest_final"] is None
    assert len(bundle["days"]) == 3


def test_1805_cli_reuses_saved_page_age_aggregate_for_lookback(tmp_path, capsys):
    from datetime import datetime
    from scrapers.fbref.daily_report import page_age_line, page_age_metrics
    cutoff = "2026-10-08T07:00:00+00:00"
    snapshot = {"competitions": [], "seasons": [], "schedules": [], "page_ages": [
        {"target_id": "index", "page_kind": "competition_index", "state": "queued",
         "refresh_policy": "daily", "last_fetched_at": cutoff}]}
    expected = page_age_metrics(snapshot, datetime.fromisoformat(cutoff))
    snapshot["page_ages"] = {"cutoff": cutoff, "by_kind": expected["by_kind"],
                            "excluded": {}, "standings_confirmed_seasons": 0}
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(snapshot))
    assert main(["--input", str(input_path), "--date", "2026-10-07", "--as-of", cutoff,
                 "--lookback", "3", "--output-dir", str(tmp_path / "out")]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 4 and lines[3] == page_age_line({"page_ages": expected})
    bundle_path = next(p for p in (tmp_path / "out").glob("*.json") if "snapshot" not in p.name)
    bundle = json.loads(bundle_path.read_text())
    assert all(report["page_ages"] == expected for report in bundle["days"])


def test_host_readers_only_select_and_keep_credentials_out_of_results(monkeypatch):
    calls = []
    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        output = '{"competitions":[]}' if argv[0] == "docker" else '"47","2026/2027","2026-10-07T20:00:00Z","2026-10-08T20:00:00Z"\n'
        return SimpleNamespace(stdout=output if argv[0] == "docker" else marked_csv(argv[1], output))
    monkeypatch.setattr("scripts.report_fbref_daily.subprocess.run", run)
    assert host_postgres("SELECT '{}'::jsonb") == {"competitions": []}
    assert "BEGIN READ ONLY" in calls[0][1]["input"] and "ON_ERROR_STOP=1" in calls[0][0]
    assert host_trino("SELECT competition_id FROM iceberg.bronze.fotmob_ingest_manifest")[0]["competition_id"] == "47"


def test_1805_host_postgres_passes_large_query_through_stdin(monkeypatch):
    query = "SELECT '" + "x" * (220 * 1024) + "'::jsonb"

    def run(argv, **kwargs):
        assert argv == ["docker", "exec", "-i", "postgres", "psql", "-U", "airflow",
                        "-d", "airflow", "-qAt", "-v", "ON_ERROR_STOP=1"]
        assert kwargs == {
            "input": "BEGIN READ ONLY; SET LOCAL statement_timeout='30s'; " + query + "; COMMIT;",
            "capture_output": True, "text": True, "timeout": 40, "check": True,
        }
        return SimpleNamespace(stdout='{"by_kind": {}}')

    monkeypatch.setattr("scripts.report_fbref_daily.subprocess.run", run)
    assert host_postgres(query) == {"by_kind": {}}


def test_host_adapter_has_no_side_effects_until_called_and_surfaces_failure(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("fbref_adapter", ROOT / "deploy/fbref/morning_report_adapter.py")
    adapter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(adapter)
    monkeypatch.setenv("FBREF_DAILY_REPORT_ROOT", str(tmp_path))
    assert "ещё не доставлен" in adapter.fbref_daily_report()[0]
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts/report_fbref_daily.py").touch()
    def fail(*args, **kwargs):
        raise OSError("private credentials must not reach Telegram")
    monkeypatch.setattr(adapter.subprocess, "run", fail)
    output = adapter.fbref_daily_report()
    assert "приёмка не подтверждена" in output[0]
    assert "credentials" not in output[0]


def test_1325_adapter_accepts_four_lines_and_rejects_old_payload(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("fbref_adapter_1325", ROOT / "deploy/fbref/morning_report_adapter.py")
    adapter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(adapter)
    monkeypatch.setenv("FBREF_DAILY_REPORT_ROOT", str(tmp_path))
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts/report_fbref_daily.py").touch()
    lines = ["FBref: матчей 1", "Последний окончательный: н/д", "Серия ≥99 %: 0 суток.",
             "Возраст по видам страниц: squad: N=0, н/д"]
    monkeypatch.setattr(adapter.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout="\n".join(lines)))
    assert adapter.fbref_daily_report() == ["• " + line for line in lines]
    lines.pop()
    assert "недоступен" in adapter.fbref_daily_report()[0]


def test_1792_reader_host_trino_four_column_manifest_contract(monkeypatch):
    from datetime import date, datetime, timezone
    from scrapers.fbref.daily_report_reader import collect_snapshot
    calls = []

    def run(argv, **kwargs):
        query = argv[1]
        calls.append(query)
        # Actual host_trino path: quoted CSV, no header, UTC timestamps,
        # and a second legacy manifest without source_observed_at evidence.
        if 'fotmob_competition_seasons' in query:
            return SimpleNamespace(stdout=marked_csv(query))
        if 'fotmob_ingest_manifest' in query:
            assert 'max(validated_at)' in query
            assert 'source_season_key AS season_key' in query
            return SimpleNamespace(stdout=marked_csv(query, (
                '"47","2026/2027","2026-10-01T00:00:00Z","2026-10-08T20:00:00Z"\n'
                '"47","2025/2026","2026-09-01T00:00:00Z","2026-09-01T00:00:00Z"\n')))
        return SimpleNamespace(stdout=marked_csv(query))

    monkeypatch.setattr('scripts.report_fbref_daily.subprocess.run', run)

    def pg(query):
        if 'competition_registry' in query:
            return {'competitions': [{'competition_id': '9', 'gender': 'male', 'name': 'Premier League'}]}
        return []

    result = collect_snapshot(pg, host_trino, day=date(2026, 10, 7),
                              as_of=datetime(2026, 10, 9, tzinfo=timezone.utc),
                              mapping={'9': {'fotmob_ids': ['47']}})
    assert not result['errors']
    assert result['fotmob_seasons'] == [
        {'competition_id': '47', 'season_key': '2026/2027',
         'fetched_at': '2026-10-01T00:00:00Z', 'validated_at': '2026-10-08T20:00:00Z'},
        {'competition_id': '47', 'season_key': '2025/2026',
         'fetched_at': '2026-09-01T00:00:00Z', 'validated_at': '2026-09-01T00:00:00Z'}]


def test_1792_round3_phase_catalog_reader_csv_contract(monkeypatch):
    from datetime import date, datetime, timezone
    from scrapers.fbref.daily_report_reader import collect_snapshot
    queries = []
    def run(argv, **kwargs):
        query = argv[1]
        queries.append(query)
        if 'fotmob_competition_seasons' in query:
            assert 's._target_batch_id=m.batch_id' in query
            assert "target_type='competition_seasons'" in query
            assert 'm.rn=1' in query and 'completed_at <=' in query
            return SimpleNamespace(stdout=marked_csv(query, '"274","2026 - Apertura","2026-10-21T04:00:00Z","2026-10-21T04:00:00Z"\n"274","2026 - Clausura","2026-10-21T04:00:00Z","2026-10-21T04:00:00Z"\n'))
        return SimpleNamespace(stdout=marked_csv(query))
    monkeypatch.setattr('scripts.report_fbref_daily.subprocess.run', run)
    def pg(query):
        if 'competition_registry' in query:
            return {'competitions': [{'competition_id': '41', 'gender': 'male', 'name': 'Primera A'}]}
        return []
    result = collect_snapshot(pg, host_trino, day=date(2026, 10, 20),
        as_of=datetime(2026, 10, 21, 5, tzinfo=timezone.utc), mapping={'41': {'fotmob_ids': ['274']}})
    assert not result['errors']
    assert {r['season_key'] for r in result['fotmob_phase_catalog']} == {'2026 - Apertura', '2026 - Clausura'}


def test_1805_host_trino_empty_success_still_requires_the_marker(monkeypatch):
    import subprocess
    import pytest
    def run(argv, **kwargs):
        assert kwargs["check"] is True
        assert "UNION ALL SELECT 'fbref_report_end'" in argv[1]
        return SimpleNamespace(stdout=marked_csv(argv[1]))
    monkeypatch.setattr("scripts.report_fbref_daily.subprocess.run", run)
    query = "SELECT target_id,content_hash FROM iceberg.bronze.fbref_table_inventory"
    assert host_trino(query) == []
    for payload in ("", '"fbref_report_data","target"\n',
                    '"fbref_report_end","bad","bad"\n', '"unterminated'):
        monkeypatch.setattr("scripts.report_fbref_daily.subprocess.run",
                            lambda *a, **k: SimpleNamespace(stdout=payload))
        with pytest.raises((ValueError, __import__("csv").Error)):
            host_trino(query)
    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, args[0])
    monkeypatch.setattr("scripts.report_fbref_daily.subprocess.run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        host_trino(query)
