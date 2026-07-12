"""
Unit tests for ``check_result_traffic_guard`` (Transfermarkt proxy budget).

The guard reads the per-entity raw decoded-byte counters and hard-fails when an
entity exceeds its residential-MiB threshold:
Airflow Variable ``tm_proxy_mb_threshold_<entity>`` → ``tm_proxy_mb_threshold``
→ per-entity default → global default.
"""

import json
from unittest.mock import patch

import pytest


@pytest.fixture
def write_result_json(tmp_path):
    """Factory that writes a run-result JSON and returns its path."""
    def _write(entity: str, mb: float) -> str:
        target = tmp_path / f"transfermarkt_{entity}_result.json"
        target.write_text(json.dumps({
            'entity': entity,
            'rows': 100,
            'traffic': {
                'telemetry_available': True,
                'decoded_response_body_bytes': int(mb * 1024 * 1024),
            },
        }))
        return str(target)
    return _write


class TestCheckResultTrafficGuard:
    @pytest.mark.unit
    def test_under_threshold_passes(self, write_result_json):
        from dags.utils.proxy_traffic import check_result_traffic_guard

        path = write_result_json('players', 4.2)
        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.return_value = None
            checked = check_result_traffic_guard(
                entity_paths={'players': path},
                default_thresholds={'players': 10.0},
            )
        assert checked['players'] == {'mb': 4.2, 'threshold_mb': 10.0}

    @pytest.mark.unit
    def test_breach_raises(self, write_result_json):
        from airflow.exceptions import AirflowException
        from dags.utils.proxy_traffic import check_result_traffic_guard

        path = write_result_json('players', 57.9)
        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.return_value = None
            with pytest.raises(AirflowException, match='players: 57.90 MB'):
                check_result_traffic_guard(
                    entity_paths={'players': path},
                    default_thresholds={'players': 10.0},
                )

    @pytest.mark.unit
    def test_per_entity_variable_overrides_default(self, write_result_json):
        from dags.utils.proxy_traffic import check_result_traffic_guard

        path = write_result_json('players', 57.9)

        def _get(key, default_var=None):
            return '100' if key == 'tm_proxy_mb_threshold_players' else default_var

        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.side_effect = _get
            checked = check_result_traffic_guard(
                entity_paths={'players': path},
                default_thresholds={'players': 10.0},
            )
        assert checked['players']['threshold_mb'] == 100.0

    @pytest.mark.unit
    def test_missing_file_fails_closed(self, write_result_json):
        from airflow.exceptions import AirflowException
        from dags.utils.proxy_traffic import check_result_traffic_guard

        ok_path = write_result_json('coaches', 1.0)
        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.return_value = None
            with pytest.raises(AirflowException, match='telemetry unavailable'):
                check_result_traffic_guard(
                    entity_paths={
                        'players': '/tmp/nonexistent-tm-result.json',
                        'coaches': ok_path,
                    },
                    default_thresholds={'coaches': 6.0},
                )

    def test_missing_traffic_counter_fails_closed(self, tmp_path):
        from airflow.exceptions import AirflowException
        from dags.utils.proxy_traffic import check_result_traffic_guard

        path = tmp_path / 'result.json'
        path.write_text(json.dumps({'entity': 'players', 'traffic': {}}))
        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.return_value = None
            with pytest.raises(AirflowException, match='decoded-byte telemetry missing'):
                check_result_traffic_guard({'players': str(path)})

    @pytest.mark.unit
    def test_rounded_mb_without_raw_bytes_fails_closed(self, tmp_path):
        from airflow.exceptions import AirflowException
        from dags.utils.proxy_traffic import check_result_traffic_guard

        path = tmp_path / 'result.json'
        path.write_text(json.dumps({
            'entity': 'players',
            'traffic': {
                'telemetry_available': True,
                'decoded_response_body_mb': 1.0,
            },
        }))
        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.return_value = None
            with pytest.raises(
                AirflowException, match='raw decoded-byte telemetry missing',
            ):
                check_result_traffic_guard({'players': str(path)})

    @pytest.mark.unit
    def test_cap_plus_one_byte_breaches_without_rounding(self, tmp_path):
        from airflow.exceptions import AirflowException
        from dags.utils.proxy_traffic import check_result_traffic_guard

        path = tmp_path / 'result.json'
        path.write_text(json.dumps({
            'entity': 'players',
            'traffic': {
                'telemetry_available': True,
                'decoded_response_body_bytes': 10 * 1024 * 1024 + 1,
                # This rounded compatibility value appears exactly at cap.
                'decoded_response_body_mb': 10.0,
            },
        }))
        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.return_value = None
            with pytest.raises(AirflowException, match='10485761 bytes'):
                check_result_traffic_guard(
                    {'players': str(path)},
                    default_thresholds={'players': 10.0},
                )

    @pytest.mark.unit
    def test_shared_cycle_budget_sums_raw_entity_bytes(self, tmp_path):
        from airflow.exceptions import AirflowException
        from dags.utils.proxy_traffic import check_result_traffic_guard

        paths = {}
        for entity in ('players', 'coaches'):
            path = tmp_path / f'{entity}.json'
            path.write_text(json.dumps({
                'entity': entity,
                'traffic': {
                    'telemetry_available': True,
                    'decoded_response_body_bytes': 8 * 1024 * 1024,
                },
            }))
            paths[entity] = str(path)
        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.return_value = None
            with pytest.raises(AirflowException, match='cycle: 16.0000 MiB'):
                check_result_traffic_guard(
                    paths,
                    default_thresholds={'players': 10.0, 'coaches': 10.0},
                    cycle_budget_bytes=15 * 1024 * 1024,
                )

    @pytest.mark.unit
    def test_cumulative_ledger_catches_overwritten_retry_result(self, tmp_path):
        from airflow.exceptions import AirflowException
        from dags.utils.proxy_traffic import check_result_traffic_guard

        path = tmp_path / 'players.json'
        path.write_text(json.dumps({
            'entity': 'players',
            'traffic': {
                'telemetry_available': True,
                'decoded_response_body_bytes': 1,
            },
            'cycle_budget': {
                'limit_bytes': 15 * 1024 * 1024,
                'accounted_after_bytes': 15 * 1024 * 1024 + 1,
            },
        }))
        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.return_value = None
            with pytest.raises(AirflowException, match='15728641 bytes'):
                check_result_traffic_guard(
                    {'players': str(path)},
                    default_thresholds={'players': 10.0},
                    cycle_budget_bytes=15 * 1024 * 1024,
                )
