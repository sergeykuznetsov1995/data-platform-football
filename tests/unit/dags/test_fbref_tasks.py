"""Unit tests for ``dags/utils/fbref_tasks.py`` factory functions (#920 Phase 1).

``task_id_suffix`` lets ``dag_ingest_fbref.py`` build a second, parallel
mini-pipeline for a single-year tournament (e.g. INT-World Cup) without
colliding with the default (club) call's task_id/output_file/traffic-output
path. Default ``''`` keeps the original task IDs while paths stay quoted and
run-scoped through ``artifact_dir``.
"""

from __future__ import annotations

import pytest


@pytest.mark.unit
class TestTaskIdSuffixDefaultsToUnchangedBehavior:
    def test_combined_season_stats_task_default_suffix(self):
        from utils.fbref_tasks import create_combined_season_stats_task

        task = create_combined_season_stats_task(
            leagues_str='ENG-Premier League', season=2025,
        )
        assert task.task_id == 'season_stats_all'
        assert '--output "/tmp/fbref_season_stats.json"' in task.bash_command
        assert (
            '--traffic-output "/tmp/fbref_traffic_season_stats.json"'
            in task.bash_command
        )

    def test_match_data_task_default_suffix(self):
        from utils.fbref_tasks import create_match_data_task

        task = create_match_data_task(
            data_type='schedule', leagues_str='ENG-Premier League',
            season=2025, scraper_type='selenium',
        )
        assert task.task_id == 'match_schedule'
        assert '--output "/tmp/fbref_match_schedule.json"' in task.bash_command
        assert (
            '--traffic-output "/tmp/fbref_traffic_match_schedule.json"'
            in task.bash_command
        )

    def test_combined_match_data_task_default_suffix(self):
        from utils.fbref_tasks import create_combined_match_data_task

        task = create_combined_match_data_task(
            leagues_str='ENG-Premier League', season=2025,
        )
        assert task.task_id == 'match_all_data'
        assert '--output "/tmp/fbref_match_all_data.json"' in task.bash_command
        assert (
            '--traffic-output "/tmp/fbref_traffic_match_all_data.json"'
            in task.bash_command
        )


@pytest.mark.unit
class TestTaskIdSuffixParametrizesEverything:
    def test_combined_season_stats_task_with_suffix(self):
        from utils.fbref_tasks import create_combined_season_stats_task

        task = create_combined_season_stats_task(
            leagues_str='INT-World Cup', season=2026, task_id_suffix='_wc',
        )
        assert task.task_id == 'season_stats_all_wc'
        assert '--output "/tmp/fbref_season_stats_wc.json"' in task.bash_command
        assert (
            '--traffic-output "/tmp/fbref_traffic_season_stats_wc.json"'
            in task.bash_command
        )

    def test_match_data_task_with_suffix(self):
        from utils.fbref_tasks import create_match_data_task

        task = create_match_data_task(
            data_type='schedule', leagues_str='INT-World Cup', season=2026,
            scraper_type='selenium', task_id_suffix='_wc',
        )
        assert task.task_id == 'match_schedule_wc'
        assert '--output "/tmp/fbref_match_schedule_wc.json"' in task.bash_command
        assert (
            '--traffic-output "/tmp/fbref_traffic_match_schedule_wc.json"'
            in task.bash_command
        )

    def test_combined_match_data_task_with_suffix(self):
        from utils.fbref_tasks import create_combined_match_data_task

        task = create_combined_match_data_task(
            leagues_str='INT-World Cup', season=2026, task_id_suffix='_wc',
        )
        assert task.task_id == 'match_all_data_wc'
        assert '--output "/tmp/fbref_match_all_data_wc.json"' in task.bash_command
        assert (
            '--traffic-output "/tmp/fbref_traffic_match_all_data_wc.json"'
            in task.bash_command
        )

    def test_default_and_suffixed_calls_never_collide(self):
        """The exact regression this suffix exists to prevent: two calls in
        the same DAG run must never write to the same output/traffic path."""
        from utils.fbref_tasks import create_combined_season_stats_task

        club = create_combined_season_stats_task(
            leagues_str='ENG-Premier League', season=2025,
        )
        tournament = create_combined_season_stats_task(
            leagues_str='INT-World Cup', season=2026, task_id_suffix='_wc',
        )
        assert club.task_id != tournament.task_id

        def _flag_value(cmd, flag):
            for line in cmd.splitlines():
                if flag in line:
                    return line.strip().split(flag)[1].split()[0]
            return None

        assert _flag_value(club.bash_command, '--output') != _flag_value(
            tournament.bash_command, '--output'
        )
        assert _flag_value(
            club.bash_command, '--traffic-output'
        ) != _flag_value(tournament.bash_command, '--traffic-output')


@pytest.mark.unit
class TestBuildCommandTrafficOutputFlag:
    """``--traffic-output`` is only emitted when a value is given — the
    runner otherwise falls back to its own /tmp/fbref_traffic_<label>.json
    default (unchanged pre-#920 behavior)."""

    def test_selenium_command_omits_flag_when_none(self):
        from utils.fbref_tasks import _build_selenium_command

        cmd = _build_selenium_command(
            mode='combined_season_stats', leagues_str='ENG-Premier League',
            season=2025, output_file='/tmp/fbref_season_stats.json',
            headless=True, use_xvfb=True, use_nodriver=True,
            nodriver_cloudflare_wait=30.0, proxy_file=None,
        )
        assert '--traffic-output' not in cmd

    def test_selenium_command_includes_flag_when_set(self):
        from utils.fbref_tasks import _build_selenium_command

        cmd = _build_selenium_command(
            mode='combined_season_stats', leagues_str='INT-World Cup',
            season=2026, output_file='/tmp/fbref_season_stats_wc.json',
            headless=True, use_xvfb=True, use_nodriver=True,
            nodriver_cloudflare_wait=30.0, proxy_file=None,
            traffic_output_file='/tmp/fbref_traffic_season_stats_wc.json',
        )
        assert (
            '--traffic-output "/tmp/fbref_traffic_season_stats_wc.json"' in cmd
        )

    def test_nodriver_command_includes_flag_when_set(self):
        from utils.fbref_tasks import _build_nodriver_command

        cmd = _build_nodriver_command(
            mode='match_data', leagues_str='INT-World Cup', season=2026,
            output_file='/tmp/fbref_match_schedule_wc.json', headless=True,
            use_xvfb=True, proxy_file=None, cloudflare_wait=30.0,
            content_timeout=45.0, max_retries=2, cf_verify_retries=6,
            match_data_type='schedule',
            traffic_output_file='/tmp/fbref_traffic_match_schedule_wc.json',
        )
        assert (
            '--traffic-output "/tmp/fbref_traffic_match_schedule_wc.json"' in cmd
        )
