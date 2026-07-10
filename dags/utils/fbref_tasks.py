"""
FBref DAG Task Factories
========================

Factory functions that create Airflow BashOperator tasks for FBref data collection.

- create_single_stat_task: Creates a task for one stat_type (player/team/keeper)
- create_match_data_task: Creates a task for one match data type (schedule, shot_events, etc.)
- create_combined_match_data_task: Creates a combined task for all match-level data in one pass
- create_trino_health_check_task: Creates a pre-flight Trino connectivity check
"""

import logging
import os
import time
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


# Common environment variables for all FBref scraper tasks
TASK_ENV = {
    'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
    'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
    'HOME': '/home/airflow',
    'DISPLAY': ':99',
    'TRINO_HOST': os.environ.get('TRINO_HOST', 'trino'),
    'TRINO_PORT': os.environ.get('TRINO_PORT', '8443'),
    'TRINO_PASSWORD': os.environ.get('TRINO_PASSWORD', ''),
    # #CF-2026-07: nodriver+Chromium 149 can no longer pass fbref's Cloudflare
    # managed interstitial. Route FBrefScraper._fetch_page through the Camoufox
    # (anti-detect Firefox) Turnstile solver. Overridable per-deploy; unset
    # (or =nodriver) reverts to the legacy path.
    'FBREF_TRANSPORT': os.environ.get('FBREF_TRANSPORT', 'camoufox'),
    # Logical identity for idempotent no-stats observations. Airflow task
    # retries render the same run_id instead of inflating confirmation count.
    'FBREF_RUN_ID': '{{ run_id }}',
}


def _build_nodriver_command(
    mode: str,
    leagues_str: str,
    season: int,
    output_file: str,
    headless: bool,
    use_xvfb: bool,
    proxy_file: str | None,
    cloudflare_wait: float,
    content_timeout: float,
    max_retries: int,
    cf_verify_retries: int,
    stat_type: str | None = None,
    data_category: str | None = None,
    match_data_type: str | None = None,
    traffic_output_file: str | None = None,
) -> str:
    """Build bash command for nodriver scraper type."""
    nodriver_args = []
    if headless:
        nodriver_args.append('--headless')
    if use_xvfb:
        nodriver_args.append('--use-xvfb')
    if proxy_file:
        nodriver_args.append(f'--proxy-file {proxy_file}')
    nodriver_args.append(f'--cloudflare-wait {cloudflare_wait}')
    nodriver_args.append(f'--content-timeout {content_timeout}')
    nodriver_args.append(f'--max-retries {max_retries}')
    nodriver_args.append(f'--cf-verify-retries {cf_verify_retries}')
    mode_args = ''
    if mode == 'single_stat':
        mode_args = (
            f'--mode single_stat \\\n'
            f'    --stat-type {stat_type} \\\n'
            f'    --data-category {data_category}'
        )
    elif mode == 'match_data':
        mode_args = (
            f'--mode match_data \\\n'
            f'    --match-data-type {match_data_type}'
        )

    traffic_arg = (
        f'--traffic-output "{traffic_output_file}"'
        if traffic_output_file else ''
    )
    output_dir = output_file.rsplit('/', 1)[0]

    return f"""
mkdir -p "{output_dir}" && \\
cd /opt/airflow && \\
python dags/scripts/run_fbref_scraper.py \\
    --scraper-type nodriver \\
    {' '.join(nodriver_args)} \\
    {mode_args} \\
    --leagues "$FBREF_LEAGUES" \\
    --season {season} {traffic_arg} \\
    --output "{output_file}"
"""


def _build_selenium_command(
    mode: str,
    leagues_str: str,
    season: int,
    output_file: str,
    headless: bool,
    use_xvfb: bool,
    use_nodriver: bool,
    nodriver_cloudflare_wait: float,
    proxy_file: str | None,
    stat_type: str | None = None,
    data_category: str | None = None,
    match_data_type: str | None = None,
    max_matches: int = 0,
    traffic_output_file: str | None = None,
) -> str:
    """Build bash command for selenium scraper type."""
    selenium_args_list = []
    if headless:
        selenium_args_list.append('--headless')
    if use_xvfb:
        selenium_args_list.append('--use-xvfb')
    if use_nodriver:
        selenium_args_list.append('--use-nodriver')
        selenium_args_list.append(f'--nodriver-cloudflare-wait {nodriver_cloudflare_wait}')
    if proxy_file:
        selenium_args_list.append(f'--proxy-file {proxy_file}')
    selenium_args = ' '.join(selenium_args_list)

    mode_args = ''
    if mode == 'single_stat':
        mode_args = (
            f'--mode single_stat \\\n'
            f'    --stat-type {stat_type} \\\n'
            f'    --data-category {data_category}'
        )
    elif mode == 'match_data':
        mode_args = (
            f'--mode match_data \\\n'
            f'    --match-data-type {match_data_type} \\\n'
            f'    --max-matches {max_matches}'
        )
    elif mode == 'combined_match_data':
        mode_args = (
            f'--mode combined_match_data \\\n'
            f'    --max-matches {max_matches}'
        )
    elif mode == 'combined_season_stats':
        mode_args = '--mode combined_season_stats'

    traffic_arg = (
        f'--traffic-output "{traffic_output_file}"'
        if traffic_output_file else ''
    )
    output_dir = output_file.rsplit('/', 1)[0]

    return f"""
mkdir -p "{output_dir}" && \\
cd /opt/airflow && \\
python dags/scripts/run_fbref_scraper.py \\
    --scraper-type selenium \\
    {selenium_args} \\
    {mode_args} \\
    --leagues "$FBREF_LEAGUES" \\
    --season {season} {traffic_arg} \\
    --output "{output_file}"
"""


def create_single_stat_task(
    stat_type: str,
    data_category: str,
    leagues_str: str,
    season: int,
    scraper_type: str = 'nodriver',
    use_xvfb: bool = True,
    headless: bool = True,
    use_nodriver: bool = True,
    nodriver_cloudflare_wait: float = 30.0,
    nodriver_content_timeout: float = 45.0,
    nodriver_max_retries: int = 2,
    nodriver_cf_verify_retries: int = 6,
    proxy_file: str | None = None,
    # Compatibility kwargs accepted but no longer used (Apr 2026):
    # the soccerdata branch was removed because curl_cffi cannot bypass
    # Cloudflare Turnstile. Kept here so existing call sites do not break.
    use_tor: bool = False,
    tor_host: str = 'tor',
    tor_port: int = 9050,
    artifact_dir: str = '/tmp',
) -> BashOperator:
    """
    Create a BashOperator task for collecting a single stat_type.

    Args:
        stat_type: The stat type to collect (e.g., 'stats', 'shooting')
        data_category: 'player', 'team', or 'keeper'
        leagues_str: Comma-separated leagues string
        season: Season year
        scraper_type: 'nodriver' (recommended) or 'selenium'
        use_xvfb: Use Xvfb virtual display
        headless: Run browser in headless mode
        use_nodriver: Use nodriver (for selenium scraper type)
        nodriver_cloudflare_wait: Time to wait for Cloudflare challenge (seconds)
        nodriver_content_timeout: Timeout for content extraction (seconds)
        nodriver_max_retries: Maximum page load retries
        nodriver_cf_verify_retries: Maximum cf-verify plugin retries
        proxy_file: Path to file with proxy list (format: host:port:user:pass)

    Returns:
        BashOperator task
    """
    # use_tor / tor_host / tor_port were used by the deprecated soccerdata
    # scraper. Silently ignore them (compat-only).
    del use_tor, tor_host, tor_port

    task_id = f'{data_category}_{stat_type}'
    output_file = f'{artifact_dir}/fbref_{task_id}.json'
    traffic_output_file = f'{artifact_dir}/fbref_traffic_{task_id}.json'

    if scraper_type == 'nodriver':
        bash_command = _build_nodriver_command(
            mode='single_stat',
            leagues_str=leagues_str,
            season=season,
            output_file=output_file,
            headless=headless,
            use_xvfb=use_xvfb,
            proxy_file=proxy_file,
            cloudflare_wait=nodriver_cloudflare_wait,
            content_timeout=nodriver_content_timeout,
            max_retries=nodriver_max_retries,
            cf_verify_retries=nodriver_cf_verify_retries,
            stat_type=stat_type,
            data_category=data_category,
            traffic_output_file=traffic_output_file,
        )
    else:
        bash_command = _build_selenium_command(
            mode='single_stat',
            leagues_str=leagues_str,
            season=season,
            output_file=output_file,
            headless=headless,
            use_xvfb=use_xvfb,
            use_nodriver=use_nodriver,
            nodriver_cloudflare_wait=nodriver_cloudflare_wait,
            proxy_file=proxy_file,
            stat_type=stat_type,
            data_category=data_category,
            traffic_output_file=traffic_output_file,
        )

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        env={
            **TASK_ENV,
            'FBREF_RUN_DIR': artifact_dir,
            'FBREF_LEAGUES': leagues_str,
        },
        append_env=True,
    )


def create_match_data_task(
    data_type: str,
    leagues_str: str,
    season: int,
    max_matches: int = 0,
    scraper_type: str = 'nodriver',
    use_xvfb: bool = True,
    headless: bool = True,
    use_nodriver: bool = True,
    nodriver_cloudflare_wait: float = 30.0,
    nodriver_content_timeout: float = 45.0,
    nodriver_max_retries: int = 2,
    nodriver_cf_verify_retries: int = 6,
    proxy_file: str | None = None,
    # Compatibility kwargs accepted but no longer used (Apr 2026):
    # the soccerdata branch was removed because curl_cffi cannot bypass
    # Cloudflare Turnstile.
    use_tor: bool = False,
    tor_host: str = 'tor',
    tor_port: int = 9050,
    task_id_suffix: str = '',
    artifact_dir: str = '/tmp',
) -> BashOperator:
    """
    Create a BashOperator task for collecting match-level data.

    Args:
        data_type: 'schedule', 'shot_events', 'match_events', or 'lineups'
        leagues_str: Comma-separated leagues string
        season: Season year
        max_matches: Maximum matches per league (0 = unlimited)
        scraper_type: 'nodriver' (recommended) or 'selenium'
        use_xvfb: Use Xvfb virtual display
        headless: Run browser in headless mode
        use_nodriver: Use nodriver (for selenium scraper type)
        nodriver_cloudflare_wait: Time to wait for Cloudflare challenge (seconds)
        nodriver_content_timeout: Timeout for content extraction (seconds)
        nodriver_max_retries: Maximum page load retries
        nodriver_cf_verify_retries: Maximum cf-verify plugin retries
        proxy_file: Path to file with proxy list (format: host:port:user:pass)
        task_id_suffix: Appended to task_id/output_file/traffic_output (#920
            Phase 1) so a second, parallel call (e.g. for a single-year
            tournament) doesn't collide with the default call's files. Empty
            by default — zero behavior change for existing callers.

    Returns:
        BashOperator task

    Note:
        For match data types other than 'schedule', nodriver scraper only
        supports 'schedule'. Use selenium for detailed match-level data
        (shot_events, match_events, lineups).
    """
    del use_tor, tor_host, tor_port  # legacy soccerdata kwargs, ignored

    task_id = f'match_{data_type}{task_id_suffix}'
    output_file = f'{artifact_dir}/fbref_{task_id}.json'
    # The runner labels its traffic-summary file f'match_{data_type}'
    # regardless of league (run_fbref_scraper.py) — must pass --traffic-output
    # explicitly so a parallel call doesn't clobber the default call's file.
    traffic_output_file = (
        f'{artifact_dir}/fbref_traffic_match_{data_type}{task_id_suffix}.json'
    )

    # For detailed match data, fall back to selenium (nodriver only supports schedule)
    effective_scraper = scraper_type
    if data_type in ['shot_events', 'match_events', 'lineups']:
        if scraper_type == 'nodriver':
            effective_scraper = 'selenium'

    if effective_scraper == 'nodriver':
        bash_command = _build_nodriver_command(
            mode='match_data',
            leagues_str=leagues_str,
            season=season,
            output_file=output_file,
            headless=headless,
            use_xvfb=use_xvfb,
            proxy_file=proxy_file,
            cloudflare_wait=nodriver_cloudflare_wait,
            content_timeout=nodriver_content_timeout,
            max_retries=nodriver_max_retries,
            cf_verify_retries=nodriver_cf_verify_retries,
            match_data_type=data_type,
            traffic_output_file=traffic_output_file,
        )
    else:
        bash_command = _build_selenium_command(
            mode='match_data',
            leagues_str=leagues_str,
            season=season,
            output_file=output_file,
            headless=headless,
            use_xvfb=use_xvfb,
            use_nodriver=use_nodriver,
            nodriver_cloudflare_wait=nodriver_cloudflare_wait,
            proxy_file=proxy_file,
            match_data_type=data_type,
            max_matches=max_matches,
            traffic_output_file=traffic_output_file,
        )

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        env={
            **TASK_ENV,
            'FBREF_RUN_DIR': artifact_dir,
            'FBREF_LEAGUES': leagues_str,
        },
        append_env=True,
    )


def create_combined_match_data_task(
    leagues_str: str,
    season: int,
    max_matches: int = 50,
    use_xvfb: bool = True,
    headless: bool = True,
    use_nodriver: bool = True,
    nodriver_cloudflare_wait: float = 30.0,
    proxy_file: str | None = None,
    task_id_suffix: str = '',
    artifact_dir: str = '/tmp',
) -> BashOperator:
    """
    Create a BashOperator task for collecting ALL match-level data in one pass.

    This task collects shot_events, match_events, and lineups simultaneously,
    reducing HTTP requests by 3x compared to separate tasks.

    Optimization:
    - Before: schedule -> shot_events -> match_events -> lineups (3 separate passes)
    - After: schedule -> match_all_data (1 combined pass)

    HTTP requests reduction:
    - Before: 1 + 380 + 380 + 380 = 1141 requests (for 380 matches)
    - After: 1 + 380 = 381 requests (3x reduction)

    Args:
        leagues_str: Comma-separated leagues string
        season: Season year
        max_matches: Maximum matches per league (default 50 for reasonable runtime)
        use_xvfb: Use Xvfb virtual display
        headless: Run browser in headless mode
        use_nodriver: Use nodriver (for selenium scraper type)
        nodriver_cloudflare_wait: Time to wait for Cloudflare challenge (seconds)
        proxy_file: Path to file with proxy list (format: host:port:user:pass)
        task_id_suffix: Appended to task_id/output_file/traffic_output (#920
            Phase 1) — see create_match_data_task. Empty by default.

    Returns:
        BashOperator task
    """
    task_id = f'match_all_data{task_id_suffix}'
    output_file = f'{artifact_dir}/fbref_match_all_data{task_id_suffix}.json'
    # Runner labels its traffic-summary file 'match_all_data' regardless of
    # league — must pass --traffic-output explicitly for a parallel call.
    traffic_output_file = (
        f'{artifact_dir}/fbref_traffic_match_all_data{task_id_suffix}.json'
    )

    bash_command = _build_selenium_command(
        mode='combined_match_data',
        leagues_str=leagues_str,
        season=season,
        output_file=output_file,
        headless=headless,
        use_xvfb=use_xvfb,
        use_nodriver=use_nodriver,
        nodriver_cloudflare_wait=nodriver_cloudflare_wait,
        proxy_file=proxy_file,
        max_matches=max_matches,
        traffic_output_file=traffic_output_file,
    )

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        env={
            **TASK_ENV,
            'FBREF_RUN_DIR': artifact_dir,
            'FBREF_LEAGUES': leagues_str,
        },
        append_env=True,
        execution_timeout=timedelta(hours=4),
    )


def create_combined_season_stats_task(
    leagues_str: str,
    season: int,
    use_xvfb: bool = True,
    headless: bool = True,
    use_nodriver: bool = True,
    nodriver_cloudflare_wait: float = 30.0,
    proxy_file: str | None = None,
    task_id_suffix: str = '',
    artifact_dir: str = '/tmp',
) -> BashOperator:
    """
    Create a BashOperator task for ALL season stats in one pass.

    Replaces the nine sequential single_stat tasks (player x4, team x4,
    keeper x1). Player and team stats share the same season page for
    stats/shooting/misc, so one process fetches 5 unique pages per
    (league, season) instead of 9 — and pays for a single CF bypass
    instead of nine (one per task process before).

    Proxy traffic per league/season: ~24 MB (9 nodriver processes)
    -> ~3.5 MB (1 CF bootstrap + 4 HTTP fast-path pages).

    Args:
        leagues_str: Comma-separated leagues string
        season: Season year
        use_xvfb: Use Xvfb virtual display
        headless: Run browser in headless mode
        use_nodriver: Use nodriver (for selenium scraper type)
        nodriver_cloudflare_wait: Time to wait for Cloudflare challenge (seconds)
        proxy_file: Path to file with proxy list (format: host:port:user:pass)
        task_id_suffix: Appended to task_id/output_file/traffic_output (#920
            Phase 1) — see create_match_data_task. Empty by default.

    Returns:
        BashOperator task
    """
    task_id = f'season_stats_all{task_id_suffix}'
    output_file = f'{artifact_dir}/fbref_season_stats{task_id_suffix}.json'
    # Runner labels its traffic-summary file 'season_stats' regardless of
    # league — must pass --traffic-output explicitly for a parallel call.
    traffic_output_file = (
        f'{artifact_dir}/fbref_traffic_season_stats{task_id_suffix}.json'
    )

    bash_command = _build_selenium_command(
        mode='combined_season_stats',
        leagues_str=leagues_str,
        season=season,
        output_file=output_file,
        headless=headless,
        use_xvfb=use_xvfb,
        use_nodriver=use_nodriver,
        nodriver_cloudflare_wait=nodriver_cloudflare_wait,
        proxy_file=proxy_file,
        traffic_output_file=traffic_output_file,
    )

    # Stale per-stat success files from the pre-combined architecture would
    # otherwise be picked up by validate_all_data (glob fbref_*.json) and
    # mask real failures.
    cleanup_cmd = (
        f'rm -f "{artifact_dir}"/fbref_player_*.json '
        f'"{artifact_dir}"/fbref_team_*.json '
        f'"{artifact_dir}"/fbref_keeper_*.json'
    )
    bash_command = f'{cleanup_cmd} && {bash_command}'

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        env={
            **TASK_ENV,
            'FBREF_RUN_DIR': artifact_dir,
            'FBREF_LEAGUES': leagues_str,
        },
        append_env=True,
        execution_timeout=timedelta(hours=2),
    )


def _check_trino_health(**kwargs):
    """Check Trino connectivity with retries.

    Uses direct `import trino` to avoid heavy scrapers/__init__.py imports
    (which pull in nodriver, selenium, soccerdata, etc. — ~1.5GB RAM).
    """
    import trino as trino_client

    host = os.environ.get('TRINO_HOST', 'trino')
    port = int(os.environ.get('TRINO_PORT', 8443))
    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD', '')

    max_attempts = 5
    backoff_base = 10  # seconds

    for attempt in range(1, max_attempts + 1):
        try:
            connect_kwargs = dict(
                host=host,
                port=port,
                user=user,
                catalog='iceberg',
            )
            if password:
                connect_kwargs.update(
                    http_scheme='https',
                    auth=trino_client.auth.BasicAuthentication(user, password),
                    verify=False,
                )

            conn = trino_client.dbapi.connect(**connect_kwargs)
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            result = cursor.fetchall()
            cursor.close()
            conn.close()

            if result and result[0][0] == 1:
                logger.info(f"Trino health check passed on attempt {attempt}")
                return True

        except Exception as e:
            backoff = backoff_base * attempt
            logger.warning(
                f"Trino health check attempt {attempt}/{max_attempts} failed: {e}. "
                f"Retrying in {backoff}s..."
            )
            if attempt < max_attempts:
                time.sleep(backoff)

    raise ConnectionError(
        f"Trino is not available after {max_attempts} attempts. "
        f"Host: {host}:{port}"
    )


def create_trino_health_check_task(
    task_id: str = 'check_trino_health',
) -> PythonOperator:
    """Create a pre-flight task to verify Trino is reachable.

    This prevents downstream tasks from silently producing 0 results
    when Trino is unavailable (schedule read from Iceberg fails → all
    leagues skipped → 0 data → exit code 0).

    Args:
        task_id: Unique task ID (allows multiple health checks in one DAG).
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=_check_trino_health,
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
