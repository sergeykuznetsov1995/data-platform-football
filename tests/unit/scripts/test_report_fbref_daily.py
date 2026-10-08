import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from scripts.report_fbref_daily import host_postgres, host_trino, main

ROOT = Path(__file__).resolve().parents[3]


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


def test_host_readers_only_select_and_keep_credentials_out_of_results(monkeypatch):
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        output = '{"competitions":[]}' if argv[0] == "docker" else '"47","2026/2027","2026-10-07T20:00:00Z"\n'
        return SimpleNamespace(stdout=output)
    monkeypatch.setattr("scripts.report_fbref_daily.subprocess.run", run)
    assert host_postgres("SELECT '{}'::jsonb") == {"competitions": []}
    assert "BEGIN READ ONLY" in calls[0][-1] and "ON_ERROR_STOP=1" in calls[0]
    assert host_trino("SELECT competition_id FROM iceberg.bronze.fotmob_ingest_manifest")[0]["competition_id"] == "47"


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
