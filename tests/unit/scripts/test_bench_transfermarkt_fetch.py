"""Unit coverage for the GET-only Transfermarkt traffic benchmark."""

from argparse import Namespace
import importlib.util
from pathlib import Path

import pandas as pd
import pytest

from scrapers.transfermarkt.models import TrafficBudgetExceeded


SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "research"
    / "bench_transfermarkt_fetch.py"
)
SPEC = importlib.util.spec_from_file_location("bench_transfermarkt_fetch", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
bench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bench)

MIB = bench.MIB
_phase_delta = bench._phase_delta
_append_checked_phase = bench._append_checked_phase
_cycle_projection = bench._cycle_projection
run = bench.run


def test_production_profile_has_exact_phase_and_shared_caps():
    from scrapers.transfermarkt import scraper as scraper_module

    assert bench.PRODUCTION_CYCLE_BUDGET_BYTES == 24 * MIB
    assert bench.PRODUCTION_PHASE_BUDGETS == {
        'squads': {
            'decoded_body_bytes': 16 * MIB,
            'request_attempts': 150,
        },
        'market_value_points': {
            'decoded_body_bytes': 4 * MIB,
            'request_attempts': 650,
        },
        'transfer_events': {
            'decoded_body_bytes': 12 * MIB,
            'request_attempts': 650,
        },
        'coaches': {
            'decoded_body_bytes': 14 * MIB,
            'request_attempts': 160,
        },
    }
    assert scraper_module._DEFAULT_DECODED_BUDGET_MB == {
        'players': 16.0,
        'market_value_history': 4.0,
        'transfers': 12.0,
        'coaches': 14.0,
    }
    assert scraper_module._DEFAULT_REQUEST_ATTEMPT_BUDGET == {
        'players': 150,
        'market_value_history': 650,
        'transfers': 650,
        'coaches': 160,
    }


def test_phase_cap_plus_one_byte_fails_on_raw_counter():
    report = {'phases': []}
    before = {'decoded_response_body_bytes': 0, 'request_attempts': 0}
    after = {
        'decoded_response_body_bytes': 16 * MIB + 1,
        'request_attempts': 1,
    }

    with pytest.raises(TrafficBudgetExceeded, match='16777217/16777216 bytes'):
        _append_checked_phase(report, 'squads', before, after)

    assert report['phases'][0]['within_budget'] is False


def test_phase_delta_uses_attempt_and_byte_differences():
    before = {
        "decoded_response_body_bytes": 100,
        "request_attempts": 2,
        "retries": 1,
        "failed_attempts": 1,
    }
    after = {
        "decoded_response_body_bytes": 100 + MIB,
        "request_attempts": 5,
        "retries": 2,
        "failed_attempts": 1,
    }

    phase = _phase_delta("sample", before, after, rows={"rows": 7})

    assert phase == {
        "phase": "sample",
        "decoded_response_body_bytes": MIB,
        "decoded_response_body_mb": 1.0,
        "request_attempts": 3,
        "retries": 1,
        "failed_attempts": 0,
        "rows": {"rows": 7},
    }


def test_cycle_projection_keeps_roster_fixed_and_scales_samples():
    assert bench.CYCLE_PLAYER_TARGET == 500
    phases = [
        {"phase": "squads", "decoded_response_body_bytes": 2 * MIB},
        {"phase": "market_value_points", "decoded_response_body_bytes": MIB},
        {"phase": "transfer_events", "decoded_response_body_bytes": 2 * MIB},
        {"phase": "coaches", "decoded_response_body_bytes": MIB},
    ]

    projected = _cycle_projection(
        phases,
        sampled_players=20,
        sampled_coach_clubs=5,
    )

    # Careers scale 20 sampled -> 500 window (x25), coaches 5 -> 20 (x4):
    # 2 fixed + 1*25 MV + 2*25 transfers + 1*4 coaches.
    assert projected["decoded_response_body_mb"] == 81.0


def test_missing_proxy_file_fails_before_network(tmp_path):
    args = Namespace(
        proxy_file=str(tmp_path / "missing.txt"),
        budget_profile='production',
        cycle_budget_bytes=15 * MIB,
        players=2,
        coach_clubs=1,
        skip_coaches=True,
        league="ENG-Premier League",
        season=2025,
    )

    code, report = run(args)

    assert code == 2
    assert report["status"] == "configuration_error"
    assert report["proxy_file"] == "missing.txt"
    assert "traffic" not in report


def test_benchmark_uses_public_empty_coach_profile_cache(
    tmp_path, monkeypatch,
):
    import scrapers.transfermarkt as transfermarkt_package

    captured = {}

    class FakeHttpClient:
        def set_cycle_decoded_body_budget(self, value):
            captured['cycle_budget_bytes'] = value

    class FakeScraper:
        def __init__(self, **kwargs):
            captured['init'] = kwargs
            self._http_client = FakeHttpClient()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def get_traffic_stats(self):
            return {
                'decoded_response_body_bytes': 0,
                'decoded_response_body_mb': 0.0,
                'request_attempts': 0,
                'retries': 0,
                'failed_attempts': 0,
            }

        def read_squad_data(self, league, season):
            memberships = pd.DataFrame([
                {
                    'club_id': '1', 'club_slug': 'a', 'club_name': 'A',
                    'player_id': '10',
                },
                {
                    'club_id': '1', 'club_slug': 'a', 'club_name': 'A',
                    'player_id': '20',
                },
            ])
            captured['memberships'] = memberships
            return {
                'memberships': memberships,
                'attribute_observations': pd.DataFrame([{'player_id': '10'}]),
            }

        def read_market_value_points(self, *args, **kwargs):
            return pd.DataFrame([{'player_id': '10'}])

        def read_transfer_events(self, *args, **kwargs):
            return pd.DataFrame([{'player_id': '10'}])

        def read_coach_data(self, *args, **kwargs):
            captured['coach_kwargs'] = kwargs
            return {
                'profiles': pd.DataFrame([{'coach_id': '7'}]),
                'stints': pd.DataFrame([{'coach_id': '7'}]),
            }

    monkeypatch.setattr(
        transfermarkt_package, 'TransfermarktScraper', FakeScraper,
    )
    proxy_file = tmp_path / 'proxies.txt'
    proxy_file.write_text('proxy.invalid:8000:user:pass\n')
    args = Namespace(
        proxy_file=str(proxy_file),
        budget_profile='production',
        cycle_budget_bytes=15 * MIB,
        players=2,
        coach_clubs=1,
        skip_coaches=False,
        league='ENG-Premier League',
        season=2025,
    )

    code, report = run(args)

    assert code == 0
    assert report['status'] == 'passed'
    assert report['cycle_budget_bytes'] == 15 * MIB
    assert captured['cycle_budget_bytes'] == 15 * MIB
    assert report['writes_iceberg'] is False
    assert captured['coach_kwargs']['coach_profile_cache'] == {}
    assert captured['coach_kwargs']['memberships'] is captured['memberships']


def test_failed_benchmark_preserves_actual_traffic_and_redacts_error(
    tmp_path, monkeypatch,
):
    import scrapers.transfermarkt as transfermarkt_package

    class FailingScraper:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def get_traffic_stats(self):
            return {
                'decoded_response_body_bytes': 1258,
                'decoded_response_body_mb': 0.0012,
                'request_attempts': 1,
                'retries': 0,
                'failed_attempts': 1,
            }

        def read_squad_data(self, league, season):
            raise RuntimeError(
                'failed via http://private:secret@proxy.invalid:1'
            )

    monkeypatch.setattr(
        transfermarkt_package, 'TransfermarktScraper', FailingScraper,
    )
    proxy_file = tmp_path / 'proxies.txt'
    proxy_file.write_text('proxy.invalid:8000:user:pass\n')
    args = Namespace(
        proxy_file=str(proxy_file),
        budget_profile='production',
        cycle_budget_bytes=15 * MIB,
        players=1,
        coach_clubs=1,
        skip_coaches=True,
        league='ENG-Premier League',
        season=2025,
    )

    code, report = run(args)

    assert code == 2
    assert report['status'] == 'failed'
    assert report['actual_decoded_response_body_mb'] == 0.0012
    assert report['traffic']['decoded_response_body_bytes'] == 1258
    assert 'private' not in report['error']
    assert 'secret' not in report['error']


def test_benchmark_fails_closed_below_coach_completeness_gate(
    tmp_path, monkeypatch,
):
    import scrapers.transfermarkt as transfermarkt_package

    class IncompleteCoachScraper:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def get_traffic_stats(self):
            return {
                'decoded_response_body_bytes': 0,
                'decoded_response_body_mb': 0.0,
                'request_attempts': 0,
                'retries': 0,
                'failed_attempts': 0,
            }

        def read_squad_data(self, league, season):
            return {
                'memberships': pd.DataFrame([
                    {'club_id': '1', 'player_id': '10'},
                    {'club_id': '2', 'player_id': '20'},
                ]),
                'attribute_observations': pd.DataFrame([{'player_id': '10'}]),
            }

        def read_market_value_points(self, *args, **kwargs):
            return pd.DataFrame([{'player_id': '10'}])

        def read_transfer_events(self, *args, **kwargs):
            return pd.DataFrame([{'player_id': '10'}])

        def read_coach_data(self, *args, **kwargs):
            return {
                'profiles': pd.DataFrame([{'coach_id': '7'}]),
                'stints': pd.DataFrame([{'coach_id': '7', 'club_id': '1'}]),
            }

    monkeypatch.setattr(
        transfermarkt_package, 'TransfermarktScraper', IncompleteCoachScraper,
    )
    proxy_file = tmp_path / 'proxies.txt'
    proxy_file.write_text('proxy.invalid:8000:user:pass\n')
    args = Namespace(
        proxy_file=str(proxy_file),
        budget_profile='production',
        cycle_budget_bytes=15 * MIB,
        players=1,
        coach_clubs=2,
        skip_coaches=False,
        league='ENG-Premier League',
        season=2025,
    )

    code, report = run(args)

    assert code == 2
    assert report['status'] == 'failed'
    assert report['coach_completeness']['ratio'] == 0.5
    assert 'below 90%' in report['error']
    assert 'cycle_projection' not in report
