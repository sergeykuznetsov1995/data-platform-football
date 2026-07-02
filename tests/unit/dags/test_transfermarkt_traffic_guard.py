"""
Unit tests for ``check_result_traffic_guard`` (Transfermarkt proxy budget).

The guard reads the per-entity run-result JSONs (``traffic.proxy_response_mb``,
#789) and hard-fails when an entity exceeds its residential-MB threshold:
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
            'traffic': {'proxy_response_mb': mb},
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
    def test_missing_file_is_skipped_not_failed(self, write_result_json):
        from dags.utils.proxy_traffic import check_result_traffic_guard

        ok_path = write_result_json('coaches', 1.0)
        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.return_value = None
            checked = check_result_traffic_guard(
                entity_paths={
                    'players': '/tmp/nonexistent-tm-result.json',
                    'coaches': ok_path,
                },
                default_thresholds={'coaches': 6.0},
            )
        assert 'players' not in checked
        assert checked['coaches']['mb'] == 1.0
