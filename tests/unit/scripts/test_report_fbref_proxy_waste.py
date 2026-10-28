import pytest
import json

from scripts.report_fbref_proxy_waste import summarize


def _boundaries():
    return {
        "start": "2026-10-28T00:00:00+00:00",
        "end": "2026-10-29T00:00:00+00:00",
        "lease_ids": ["one"],
        "warmup": [{
            "lease_id": "one",
            "start": "2026-10-28T00:00:00+00:00",
            "end": "2026-10-28T01:00:00+00:00",
        }],
    }


def _event(host, count, hour="00", **kwargs):
    return dict(event_version="paid-proxy-v2", event_type="bytes", source="fbref",
                lease_id="one", occurred_at=f"2026-10-28T{hour}:00:00+00:00",
                host=host, bytes=count, **kwargs)


def test_cf_shares_have_distinct_denominators_and_explicit_period():
    events = [_event("challenges.cloudflare.com", 30), _event("fbref.com", 70),
              _event("fbref.com", 180, "01"), _event("cloudflare.com", 20, "02"),
              _event("cloudflare.com.attacker.test", 200, "03")]
    foreign = _event("cloudflare.com", 999)
    foreign["lease_id"] = "other"
    events.append(foreign)
    result = summarize(events, _boundaries())
    assert result["contour"] == {
        "label": "CF / весь трафик выбранного контура", "cloudflare_bytes": 50,
        "total_bytes": 500, "percent": 10, "status": "available",
    }
    assert result["warmup"] == {
        "label": "CF в прогреве / весь прогрев", "cloudflare_bytes": 30,
        "total_bytes": 100, "percent": 30, "status": "available",
    }
    assert result["period"]["end"] == _boundaries()["end"]


def test_no_bytes_and_missing_phase_boundaries_are_explicit():
    result = summarize([], _boundaries())
    assert result["contour"]["percent"] is None
    assert result["warmup"]["status"] == "no_data"
    boundaries = _boundaries()
    del boundaries["warmup"]
    result = summarize([_event("fbref.com", 100)], boundaries)
    assert result["warmup"]["total_bytes"] is None
    assert result["warmup"]["cloudflare_bytes"] is None
    assert result["warmup"]["percent"] is None


def test_invalid_bytes_and_overlapping_phases_are_rejected():
    with pytest.raises(ValueError, match="nonnegative integer"):
        summarize([_event("fbref.com", -1)], _boundaries())
    boundaries = _boundaries()
    boundaries["warmup"] *= 2
    with pytest.raises(ValueError, match="overlapping"):
        summarize([], boundaries)


def test_cli_reads_saved_ledger_and_boundaries(tmp_path, monkeypatch, capsys):
    from scripts.report_fbref_proxy_waste import main

    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(json.dumps(_event("challenges.cloudflare.com", 40)) + "\n")
    boundaries = tmp_path / "boundaries.json"
    boundaries.write_text(json.dumps(_boundaries()))
    monkeypatch.setattr("sys.argv", ["report", "--ledger", str(ledger),
                                     "--boundaries", str(boundaries)])
    main()
    report = json.loads(capsys.readouterr().out)
    assert report["contour"]["total_bytes"] == 40
    assert report["warmup"]["percent"] == 100
