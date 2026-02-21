"""
FBref DAG Callbacks
===================

Callback and callable functions for FBref DAG PythonOperator tasks.

- prewarm_cf_cookies: Pre-solve Cloudflare Turnstile before scraper tasks
- validate_all_data: Validate all scraped data after TaskGroups complete
"""

from typing import Any, Dict


def prewarm_cf_cookies(
    proxy_file: str,
    cache_ttl_minutes: int,
    use_cf_verify: bool,
    cf_verify_max_retries: int,
    cf_verify_interval: float,
    use_xvfb: bool,
    max_attempts: int,
    **context,
) -> Dict[str, Any]:
    """
    Pre-solve Cloudflare Turnstile before starting scraper tasks.

    Uses nodriver + cf-verify plugin to obtain cf_clearance cookies,
    then stores them in XCom for use by scraper tasks.

    Args:
        proxy_file: Path to proxy file (format: host:port:user:pass)
        cache_ttl_minutes: CF cookie cache TTL in minutes
        use_cf_verify: Whether to use cf-verify plugin
        cf_verify_max_retries: Max retries for cf-verify
        cf_verify_interval: Interval between cf-verify retries (seconds)
        use_xvfb: Whether to use Xvfb virtual display
        max_attempts: Maximum cookie prewarm attempts across proxies
        **context: Airflow context (passed by PythonOperator)

    Returns:
        Dictionary with success status and cookie info
    """
    import logging

    logger = logging.getLogger(__name__)
    logger.info("Pre-warming CF cookies for FBref...")

    result = {
        'success': False,
        'cookie_count': 0,
        'cookies': {},
        'error': None,
    }

    try:
        from scrapers.base.browser.cf_cookie_manager import CFCookieManager
        from scrapers.utils.proxy_manager import ProxyManager, ProxyType
    except ImportError as e:
        logger.error(f"Failed to import required modules: {e}")
        result['error'] = str(e)
        return result

    try:
        # Load proxies
        proxy_manager = ProxyManager()
        proxy_manager.load_from_file_custom_format(proxy_file, ProxyType.HTTP)
        logger.info(f"Loaded {proxy_manager.total_count} proxies for CF prewarm")

        # Create CF cookie manager with optimized settings
        manager = CFCookieManager(
            cache_ttl_minutes=cache_ttl_minutes,
            use_cf_verify=use_cf_verify,
            cf_verify_max_retries=cf_verify_max_retries,
            cf_verify_interval=cf_verify_interval,
            use_xvfb=use_xvfb,
        )

        # Get cookies with retry across different proxies
        cookies = manager.get_cookies_with_retry_sync(
            url="https://fbref.com/en/",
            proxy_manager=proxy_manager,
            max_attempts=max_attempts,
        )

        if cookies and 'cf_clearance' in cookies:
            # Success - push cookies to XCom for scraper tasks
            context['ti'].xcom_push(key='cf_cookies', value=cookies)
            result['success'] = True
            result['cookie_count'] = len(cookies)
            result['cookies'] = list(cookies.keys())
            logger.info(
                f"CF prewarm successful: {len(cookies)} cookies obtained: "
                f"{list(cookies.keys())}"
            )
        else:
            logger.warning(
                f"CF prewarm failed - no cf_clearance cookie obtained. "
                f"Got cookies: {list(cookies.keys()) if cookies else 'none'}"
            )
            result['error'] = 'No cf_clearance cookie obtained'

    except Exception as e:
        logger.error(f"CF prewarm failed with exception: {e}")
        result['error'] = str(e)

    return result


def validate_all_data(**context) -> Dict[str, Any]:
    """
    Validate all scraped data from all TaskGroups.

    Checks /tmp/fbref_*.json result files and validates minimum
    data thresholds (at least 10 out of expected ~24 tables).

    Args:
        **context: Airflow context (passed by PythonOperator)

    Returns:
        Validation results dictionary

    Raises:
        AirflowException: If validation fails (no tables collected)
    """
    import json
    import logging
    from pathlib import Path

    from airflow.exceptions import AirflowException

    logger = logging.getLogger(__name__)

    validation = {
        'status': 'success',
        'warnings': [],
        'tables_collected': [],
        'errors': [],
    }

    # Check all result files
    result_dir = Path('/tmp')

    for result_file in result_dir.glob('fbref_*.json'):
        try:
            with open(result_file, 'r') as f:
                result = json.load(f)

            tables = result.get('tables', [])
            errors = result.get('errors', [])

            if tables:
                validation['tables_collected'].extend(tables)

            if errors:
                validation['errors'].extend(errors)
                validation['warnings'].append(
                    f"{result_file.name}: {len(errors)} error(s)"
                )

        except (FileNotFoundError, json.JSONDecodeError) as e:
            validation['warnings'].append(f"Error reading {result_file}: {e}")

    # Check minimum data thresholds
    total_tables = len(validation['tables_collected'])

    # We expect at least:
    # - 9 player tables + 9 team tables + 2 keeper tables + 4 match data tables = 24 tables
    # But some may fail, so we accept >= 10 as partial success
    if total_tables == 0:
        validation['status'] = 'failed'
        validation['warnings'].append('No tables were collected')
    elif total_tables < 10:
        validation['status'] = 'partial_success'
        validation['warnings'].append(
            f"Only {total_tables} tables collected (expected ~24)"
        )
    else:
        logger.info(f"Collected {total_tables} tables successfully")

    if validation['errors']:
        if validation['status'] == 'success':
            validation['status'] = 'partial_success'

    logger.info(f"Validation complete: {validation['status']}")
    logger.info(f"Tables collected: {total_tables}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation}")

    return validation
