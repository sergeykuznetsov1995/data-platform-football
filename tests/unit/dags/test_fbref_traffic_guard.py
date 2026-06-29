"""
Unit tests for `check_traffic_guard` parameterization (issue #44).

The guard now serves any FBref task — match_all_data, match_schedule,
player_*, team_*, keeper_*. Each task has its own JSON path and
optional per-task threshold Airflow Variable.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def write_traffic_json(tmp_path):
    """Factory that writes a traffic JSON and returns its path."""
    def _write(label: str, payload: dict) -> str:
        target = tmp_path / f"fbref_traffic_{label}.json"
        target.write_text(json.dumps(payload))
        return str(target)
    return _write


@pytest.fixture
def airflow_ti():
    """Lightweight fake task instance recording xcom_push calls."""
    ti = MagicMock()
    ti.pushed = {}

    def _push(key, value):
        ti.pushed[key] = value
    ti.xcom_push.side_effect = _push
    return ti


class TestCheckTrafficGuardParameterized:
    """The guard must read the JSON pointed to by op_kwargs, not a hard path."""

    @pytest.mark.unit
    def test_reads_custom_path_and_pushes_extended_xcom(
        self, write_traffic_json, airflow_ti
    ):
        from dags.utils.fbref_callbacks import check_traffic_guard

        path = write_traffic_json('player_stats', {
            'mode': 'single_stat',
            'label': 'player_stats',
            'real_proxy_mb': 12.5,
            'real_proxy_bytes': 13_107_200,
            'real_proxy_requests': 100,
            'successes': 5,
            'cf_challenge_attempts': 3,
            'cf_challenges_passed': 3,
            'cf_challenges_failed': 0,
            'restart_reasons': {'page_limit': 1},
            'real_proxy_mb_by_resource_type': {'Document': 8.0, 'Script': 4.5},
        })

        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.return_value = '500'  # global fallback
            result = check_traffic_guard(
                traffic_path=path,
                label='player_stats',
                ti=airflow_ti,
            )

        assert result['status'] == 'ok'
        assert result['label'] == 'player_stats'
        assert result['real_proxy_mb'] == 12.5
        assert result['cf_challenge_attempts'] == 3
        assert result['restart_reasons'] == {'page_limit': 1}

        assert airflow_ti.pushed['real_proxy_mb'] == 12.5
        assert airflow_ti.pushed['cf_challenges_passed'] == 3
        assert airflow_ti.pushed['restart_reasons'] == {'page_limit': 1}
        assert airflow_ti.pushed['real_proxy_mb_by_resource_type'] == {
            'Document': 8.0, 'Script': 4.5,
        }

    @pytest.mark.unit
    def test_per_task_variable_overrides_global(
        self, write_traffic_json, airflow_ti
    ):
        from dags.utils.fbref_callbacks import check_traffic_guard

        path = write_traffic_json('match_schedule', {
            'real_proxy_mb': 25.0,
            'real_proxy_requests': 12,
        })

        # Per-task Variable says 50 MB — well above 25 MB observed.
        # Global says 10 MB — would have failed if used.
        def fake_get(name, default_var=None):
            return {
                'fbref_proxy_mb_threshold_match_schedule': '50',
                'fbref_proxy_mb_threshold': '10',
            }.get(name, default_var)

        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.side_effect = fake_get
            result = check_traffic_guard(
                traffic_path=path,
                label='match_schedule',
                ti=airflow_ti,
            )

        assert result['status'] == 'ok'
        assert result['threshold_mb'] == 50.0

    @pytest.mark.unit
    def test_raises_when_real_mb_above_threshold(
        self, write_traffic_json, airflow_ti
    ):
        from airflow.exceptions import AirflowException
        from dags.utils.fbref_callbacks import check_traffic_guard

        path = write_traffic_json('match_all_data', {
            'real_proxy_mb': 750.0,
            'real_proxy_requests': 5000,
        })

        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.return_value = '500'
            with pytest.raises(AirflowException) as exc_info:
                check_traffic_guard(
                    traffic_path=path,
                    label='match_all_data',
                    ti=airflow_ti,
                )

        assert 'match_all_data' in str(exc_info.value)
        assert '750' in str(exc_info.value)

    @pytest.mark.unit
    def test_missing_file_returns_status_missing(self, tmp_path, airflow_ti):
        from dags.utils.fbref_callbacks import check_traffic_guard

        path = str(tmp_path / "does_not_exist.json")

        with patch('airflow.models.Variable') as mock_var:
            mock_var.get.return_value = '500'
            result = check_traffic_guard(
                traffic_path=path,
                label='match_all_data',
                ti=airflow_ti,
            )

        assert result['status'] == 'missing'
        assert result['real_proxy_mb'] is None
