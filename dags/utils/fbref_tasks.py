"""
FBref DAG Task Factories
========================

Factory functions that create Airflow BashOperator tasks for FBref data collection.

- create_single_stat_task: Creates a task for one stat_type (player/team/keeper)
- create_match_data_task: Creates a task for one match data type (schedule, shot_events, etc.)
- create_combined_match_data_task: Creates a combined task for all match-level data in one pass
"""

from airflow.operators.bash import BashOperator


# Common environment variables for all FBref scraper tasks
TASK_ENV = {
    'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
    'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
    'HOME': '/home/airflow',
    'DISPLAY': ':99',
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

    return f"""
cd /opt/airflow && \\
python dags/scripts/run_fbref_scraper.py \\
    --scraper-type nodriver \\
    {' '.join(nodriver_args)} \\
    {mode_args} \\
    --leagues "{leagues_str}" \\
    --season {season} \\
    --output {output_file}
"""


def _build_soccerdata_command(
    mode: str,
    leagues_str: str,
    season: int,
    output_file: str,
    use_tor: bool,
    tor_host: str,
    tor_port: int,
    proxy_file: str | None,
    stat_type: str | None = None,
    data_category: str | None = None,
    match_data_type: str | None = None,
) -> str:
    """Build bash command for soccerdata scraper type (deprecated)."""
    tor_args = f'--use-tor --tor-host {tor_host} --tor-port {tor_port}' if use_tor else ''
    proxy_args = f'--proxy-file {proxy_file}' if proxy_file else ''

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

    return f"""
cd /opt/airflow && \\
python dags/scripts/run_fbref_scraper.py \\
    --scraper-type soccerdata \\
    {tor_args} \\
    {proxy_args} \\
    {mode_args} \\
    --leagues "{leagues_str}" \\
    --season {season} \\
    --output {output_file}
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

    return f"""
cd /opt/airflow && \\
python dags/scripts/run_fbref_scraper.py \\
    --scraper-type selenium \\
    {selenium_args} \\
    {mode_args} \\
    --leagues "{leagues_str}" \\
    --season {season} \\
    --output {output_file}
"""


def create_single_stat_task(
    stat_type: str,
    data_category: str,
    leagues_str: str,
    season: int,
    scraper_type: str = 'nodriver',
    use_tor: bool = False,
    tor_host: str = 'tor',
    tor_port: int = 9050,
    use_xvfb: bool = True,
    headless: bool = True,
    use_nodriver: bool = True,
    nodriver_cloudflare_wait: float = 30.0,
    nodriver_content_timeout: float = 45.0,
    nodriver_max_retries: int = 2,
    nodriver_cf_verify_retries: int = 6,
    proxy_file: str | None = None,
) -> BashOperator:
    """
    Create a BashOperator task for collecting a single stat_type.

    Args:
        stat_type: The stat type to collect (e.g., 'stats', 'shooting', 'passing')
        data_category: 'player', 'team', or 'keeper'
        leagues_str: Comma-separated leagues string
        season: Season year
        scraper_type: 'nodriver' (recommended), 'soccerdata' (deprecated), 'selenium'
        use_tor: Use Tor proxy for soccerdata scraper (deprecated)
        tor_host: Tor SOCKS5 proxy host
        tor_port: Tor SOCKS5 proxy port
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
    task_id = f'{data_category}_{stat_type}'
    output_file = f'/tmp/fbref_{task_id}.json'

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
        )
    elif scraper_type == 'soccerdata':
        bash_command = _build_soccerdata_command(
            mode='single_stat',
            leagues_str=leagues_str,
            season=season,
            output_file=output_file,
            use_tor=use_tor,
            tor_host=tor_host,
            tor_port=tor_port,
            proxy_file=proxy_file,
            stat_type=stat_type,
            data_category=data_category,
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
        )

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        env=TASK_ENV,
    )


def create_match_data_task(
    data_type: str,
    leagues_str: str,
    season: int,
    max_matches: int = 0,
    scraper_type: str = 'nodriver',
    use_tor: bool = False,
    tor_host: str = 'tor',
    tor_port: int = 9050,
    use_xvfb: bool = True,
    headless: bool = True,
    use_nodriver: bool = True,
    nodriver_cloudflare_wait: float = 30.0,
    nodriver_content_timeout: float = 45.0,
    nodriver_max_retries: int = 2,
    nodriver_cf_verify_retries: int = 6,
    proxy_file: str | None = None,
) -> BashOperator:
    """
    Create a BashOperator task for collecting match-level data.

    Args:
        data_type: 'schedule', 'shot_events', 'match_events', or 'lineups'
        leagues_str: Comma-separated leagues string
        season: Season year
        max_matches: Maximum matches per league (0 = unlimited)
        scraper_type: 'nodriver' (recommended), 'soccerdata' (deprecated), 'selenium'
        use_tor: Use Tor proxy for soccerdata scraper (deprecated)
        tor_host: Tor SOCKS5 proxy host
        tor_port: Tor SOCKS5 proxy port
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

    Note:
        For match data types other than 'schedule', nodriver scraper only
        supports 'schedule'. Use selenium for detailed match-level data
        (shot_events, match_events, lineups).
    """
    task_id = f'match_{data_type}'
    output_file = f'/tmp/fbref_{task_id}.json'

    # For detailed match data, fall back to selenium (nodriver only supports schedule)
    effective_scraper = scraper_type
    if data_type in ['shot_events', 'match_events', 'lineups']:
        if scraper_type in ['nodriver', 'soccerdata']:
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
        )
    elif effective_scraper == 'soccerdata':
        bash_command = _build_soccerdata_command(
            mode='match_data',
            leagues_str=leagues_str,
            season=season,
            output_file=output_file,
            use_tor=use_tor,
            tor_host=tor_host,
            tor_port=tor_port,
            proxy_file=proxy_file,
            match_data_type=data_type,
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
        )

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        env=TASK_ENV,
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

    Returns:
        BashOperator task
    """
    task_id = 'match_all_data'
    output_file = '/tmp/fbref_match_all_data.json'

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
    )

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        env=TASK_ENV,
    )
