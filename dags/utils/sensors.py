"""
Custom Airflow Sensors
======================

Sensors for checking external dependencies before running tasks.
"""

import logging
from typing import Optional

import requests
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)


class FlareSolverrSensor(BaseSensorOperator):
    """
    Sensor that checks if FlareSolverr service is healthy and responsive.

    FlareSolverr is used to bypass Cloudflare protection when scraping websites.
    This sensor ensures the service is available before starting scraping tasks.

    Example usage:
        flaresolverr_check = FlareSolverrSensor(
            task_id='check_flaresolverr',
            flaresolverr_url='http://flaresolverr:8191',
            timeout=30,
        )
    """

    template_fields = ('flaresolverr_url',)

    @apply_defaults
    def __init__(
        self,
        flaresolverr_url: str = 'http://flaresolverr:8191',
        request_timeout: int = 30,
        *args,
        **kwargs
    ):
        """
        Initialize the sensor.

        Args:
            flaresolverr_url: URL of the FlareSolverr service
            request_timeout: Timeout for health check request in seconds
        """
        super().__init__(*args, **kwargs)
        self.flaresolverr_url = flaresolverr_url
        self.request_timeout = request_timeout

    def poke(self, context) -> bool:
        """
        Check if FlareSolverr is healthy.

        Returns:
            True if FlareSolverr is healthy, False otherwise
        """
        health_url = f"{self.flaresolverr_url.rstrip('/')}/health"

        try:
            logger.info(f"Checking FlareSolverr health: {health_url}")

            response = requests.get(
                health_url,
                timeout=self.request_timeout
            )

            if response.status_code == 200:
                data = response.json()
                status = data.get('status', 'unknown')

                if status == 'ok':
                    logger.info(
                        f"FlareSolverr is healthy: {data}"
                    )
                    return True
                else:
                    logger.warning(
                        f"FlareSolverr status is not 'ok': {data}"
                    )
                    return False
            else:
                logger.warning(
                    f"FlareSolverr health check failed: "
                    f"status={response.status_code}, response={response.text}"
                )
                return False

        except requests.exceptions.ConnectionError as e:
            logger.warning(
                f"Cannot connect to FlareSolverr at {health_url}: {e}"
            )
            return False
        except requests.exceptions.Timeout:
            logger.warning(
                f"FlareSolverr health check timed out after {self.request_timeout}s"
            )
            return False
        except Exception as e:
            logger.error(
                f"Unexpected error checking FlareSolverr health: {e}"
            )
            return False


def check_flaresolverr_health(
    flaresolverr_url: str = 'http://flaresolverr:8191',
    timeout: int = 30,
    raise_on_failure: bool = True
) -> bool:
    """
    Simple function to check FlareSolverr health.

    Can be used in PythonOperator or as pre-task check.

    Args:
        flaresolverr_url: URL of FlareSolverr service
        timeout: Request timeout in seconds
        raise_on_failure: If True, raise exception on failure

    Returns:
        True if healthy

    Raises:
        RuntimeError: If raise_on_failure=True and health check fails
    """
    health_url = f"{flaresolverr_url.rstrip('/')}/health"

    try:
        response = requests.get(health_url, timeout=timeout)

        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'ok':
                logger.info(f"FlareSolverr is healthy: {data}")
                return True

        error_msg = (
            f"FlareSolverr health check failed: "
            f"status={response.status_code}, response={response.text}"
        )

    except requests.exceptions.ConnectionError as e:
        error_msg = f"Cannot connect to FlareSolverr at {health_url}: {e}"
    except requests.exceptions.Timeout:
        error_msg = f"FlareSolverr health check timed out after {timeout}s"
    except Exception as e:
        error_msg = f"Unexpected error: {e}"

    logger.error(error_msg)

    if raise_on_failure:
        raise RuntimeError(error_msg)

    return False
