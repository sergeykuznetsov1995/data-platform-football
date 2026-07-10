"""Run-scoped, fail-closed validation for the FBref ingest DAG."""

import json

import pytest


SEASON_TABLES = [
    'fbref_player_stats',
    'fbref_player_shooting',
    'fbref_player_playingtime',
    'fbref_player_misc',
    'fbref_team_stats',
    'fbref_team_shooting',
    'fbref_team_playingtime',
    'fbref_team_misc',
    'fbref_keeper_keeper',
]
MATCH_TABLES = [
    'fbref_match_team_stats',
    'fbref_match_player_stats',
    'fbref_match_events',
    'fbref_lineups',
]


def _write_manifest(directory, filename, tables, **extra):
    payload = {
        'tables': [f'iceberg.bronze.{table}' for table in tables],
        'errors': [],
        'fallback_files': [],
    }
    payload.update(extra)
    (directory / filename).write_text(json.dumps(payload))


def _write_complete_run(directory, suffix=''):
    _write_manifest(
        directory, f'fbref_season_stats{suffix}.json', SEASON_TABLES
    )
    _write_manifest(
        directory, f'fbref_match_schedule{suffix}.json', ['fbref_schedule']
    )
    _write_manifest(
        directory, f'fbref_match_all_data{suffix}.json', MATCH_TABLES
    )


@pytest.mark.unit
def test_qualified_iceberg_paths_satisfy_current_run_contract(tmp_path):
    from dags.utils.fbref_callbacks import validate_all_data

    _write_complete_run(tmp_path)

    result = validate_all_data(result_dir=str(tmp_path))

    assert result['status'] == 'success'
    assert result['missing_tables'] == []


@pytest.mark.unit
def test_missing_current_manifest_fails_even_if_stale_run_is_complete(tmp_path):
    from airflow.exceptions import AirflowException
    from dags.utils.fbref_callbacks import validate_all_data

    stale = tmp_path / 'stale'
    current = tmp_path / 'current'
    stale.mkdir()
    current.mkdir()
    _write_complete_run(stale)

    with pytest.raises(AirflowException, match='Missing current-run'):
        validate_all_data(result_dir=str(current))


@pytest.mark.unit
def test_local_fallback_is_not_counted_as_an_iceberg_table(tmp_path):
    from airflow.exceptions import AirflowException
    from dags.utils.fbref_callbacks import validate_all_data

    _write_complete_run(tmp_path)
    _write_manifest(
        tmp_path,
        'fbref_match_all_data.json',
        MATCH_TABLES,
        fallback_files=['/tmp/current/fbref_batch_lineups.json'],
    )

    with pytest.raises(AirflowException, match='fallback'):
        validate_all_data(result_dir=str(tmp_path))


@pytest.mark.unit
def test_manifest_errors_fail_validation(tmp_path):
    from airflow.exceptions import AirflowException
    from dags.utils.fbref_callbacks import validate_all_data

    _write_complete_run(tmp_path)
    _write_manifest(
        tmp_path,
        'fbref_season_stats.json',
        SEASON_TABLES,
        errors=['Trino write failed'],
    )

    with pytest.raises(AirflowException, match='Trino write failed'):
        validate_all_data(result_dir=str(tmp_path))


@pytest.mark.unit
def test_each_tournament_scope_must_have_its_own_manifests(tmp_path):
    from airflow.exceptions import AirflowException
    from dags.utils.fbref_callbacks import validate_all_data

    _write_complete_run(tmp_path)
    _write_manifest(
        tmp_path,
        'fbref_season_stats_int_world_cup.json',
        SEASON_TABLES,
    )

    with pytest.raises(AirflowException, match='int_world_cup'):
        validate_all_data(
            result_dir=str(tmp_path),
            manifest_suffixes=['', '_int_world_cup'],
        )


@pytest.mark.unit
def test_consistent_out_of_window_tournament_is_valid_noop(tmp_path):
    from dags.utils.fbref_callbacks import validate_all_data

    _write_complete_run(tmp_path)
    for stem in (
        'fbref_season_stats',
        'fbref_match_schedule',
        'fbref_match_all_data',
    ):
        _write_manifest(
            tmp_path,
            f'{stem}_int_world_cup.json',
            [],
            skipped='out_of_window',
        )

    result = validate_all_data(
        result_dir=str(tmp_path),
        manifest_suffixes=['', '_int_world_cup'],
    )

    assert result['status'] == 'success'
    assert result['skipped_scopes'] == ['int_world_cup']
