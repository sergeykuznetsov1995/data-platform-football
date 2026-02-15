"""
FlareSolverr client для обхода Cloudflare protection.

FlareSolverr - это proxy server, который использует puppeteer
для решения Cloudflare challenges (включая Turnstile CAPTCHA).
"""

import logging
import requests
from typing import Optional, Dict, Any
from urllib.parse import urlparse

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

logger = logging.getLogger(__name__)


# Retry configuration for FlareSolverr requests
FLARESOLVERR_RETRY_ATTEMPTS = 3
FLARESOLVERR_RETRY_WAIT_MIN = 5
FLARESOLVERR_RETRY_WAIT_MAX = 30


class FlareSolverrClient:
    """Клиент для взаимодействия с FlareSolverr API."""

    def __init__(
        self,
        base_url: str = "http://flaresolverr:8191",
        max_timeout: int = 120000,
        retry_attempts: int = FLARESOLVERR_RETRY_ATTEMPTS,
    ):
        """
        Инициализация клиента.

        Args:
            base_url: URL FlareSolverr сервиса
            max_timeout: Максимальный таймаут в миллисекундах (default: 120000)
            retry_attempts: Количество попыток при ошибках (default: 3)
        """
        self.base_url = base_url.rstrip('/')
        self.max_timeout = max_timeout
        self.retry_attempts = retry_attempts
        self._session_id: Optional[str] = None

    def _convert_proxy_format(self, proxy: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Конвертирует прокси из формата http://user:pass@host:port
        в формат FlareSolverr с раздельными username/password.

        FlareSolverr требует формат:
        {
            "url": "http://host:port",
            "username": "user",
            "password": "pass"
        }

        Args:
            proxy: Прокси строка в формате http://user:pass@host:port или None

        Returns:
            Dict с url и credentials для FlareSolverr или None
        """
        if not proxy:
            return None

        parsed = urlparse(proxy)

        # Если есть username/password, разделяем их
        if parsed.username and parsed.password:
            # Собираем URL без credentials
            base_url = f"{parsed.scheme}://{parsed.hostname}"
            if parsed.port:
                base_url += f":{parsed.port}"

            return {
                "url": base_url,
                "username": parsed.username,
                "password": parsed.password
            }

        # Если нет credentials, просто возвращаем url
        return {"url": proxy}

    def request_get(
        self,
        url: str,
        proxy: Optional[str] = None,
        max_timeout: Optional[int] = None,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Выполняет GET запрос через FlareSolverr с retry логикой.

        Args:
            url: URL для запроса
            proxy: Прокси в формате http://user:pass@host:port
            max_timeout: Таймаут в мс (по умолчанию self.max_timeout)
            session_id: ID сессии для переиспользования cookies

        Returns:
            Response dict с ключами: status, message, solution
            solution содержит: url, status, headers, response (HTML), cookies

        Raises:
            requests.RequestException: При ошибке сети
            ValueError: При ошибке FlareSolverr
        """
        return self._request_get_with_retry(url, proxy, max_timeout, session_id)

    @retry(
        stop=stop_after_attempt(FLARESOLVERR_RETRY_ATTEMPTS),
        wait=wait_exponential(
            multiplier=1,
            min=FLARESOLVERR_RETRY_WAIT_MIN,
            max=FLARESOLVERR_RETRY_WAIT_MAX
        ),
        retry=retry_if_exception_type((
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _request_get_with_retry(
        self,
        url: str,
        proxy: Optional[str] = None,
        max_timeout: Optional[int] = None,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Internal method with retry decorator for FlareSolverr requests.
        """
        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": max_timeout or self.max_timeout
        }

        proxy_dict = self._convert_proxy_format(proxy)
        if proxy_dict:
            payload["proxy"] = proxy_dict

        if session_id:
            payload["session"] = session_id

        logger.debug(f"FlareSolverr request: {url}")

        timeout_seconds = (max_timeout or self.max_timeout) / 1000 + 30

        response = requests.post(
            f"{self.base_url}/v1",
            json=payload,
            timeout=timeout_seconds
        )
        response.raise_for_status()

        result = response.json()

        if result.get("status") != "ok":
            error_msg = result.get("message", "Unknown FlareSolverr error")
            logger.error(
                f"FlareSolverr error for {url}: {error_msg}. "
                f"Full response: {result}"
            )
            raise ValueError(f"FlareSolverr error: {error_msg}")

        solution = result.get('solution', {})
        response_html = solution.get('response', '')
        html_len = len(response_html) if response_html else 0

        # Check for Cloudflare challenge in response
        cloudflare_indicators = [
            'just a moment', 'checking your browser',
            'cf-browser-verification', 'challenge-running',
            'attention required'
        ]
        response_lower = response_html.lower() if response_html else ''
        has_cloudflare = any(cf in response_lower for cf in cloudflare_indicators)

        logger.info(
            f"FlareSolverr success: {url}, "
            f"http_status={solution.get('status')}, "
            f"html_length={html_len}, "
            f"has_cloudflare_challenge={has_cloudflare}"
        )

        if has_cloudflare:
            logger.warning(
                f"FlareSolverr returned Cloudflare challenge page for {url}. "
                f"Preview: {response_html[:500] if response_html else 'empty'}"
            )

        return result

    def create_session(self, proxy: Optional[str] = None) -> str:
        """
        Создаёт сессию FlareSolverr для переиспользования cookies.

        Args:
            proxy: Прокси для сессии

        Returns:
            Session ID
        """
        payload = {
            "cmd": "sessions.create"
        }

        proxy_dict = self._convert_proxy_format(proxy)
        if proxy_dict:
            payload["proxy"] = proxy_dict

        response = requests.post(
            f"{self.base_url}/v1",
            json=payload,
            timeout=60
        )
        response.raise_for_status()

        result = response.json()

        if result.get("status") != "ok":
            raise ValueError(f"Failed to create session: {result.get('message')}")

        session_id = result.get("session")
        self._session_id = session_id
        logger.info(f"Created FlareSolverr session: {session_id}")

        return session_id

    def destroy_session(self, session_id: Optional[str] = None) -> bool:
        """
        Уничтожает сессию FlareSolverr.

        Args:
            session_id: ID сессии (по умолчанию текущая)

        Returns:
            True если успешно
        """
        sid = session_id or self._session_id
        if not sid:
            return True

        payload = {
            "cmd": "sessions.destroy",
            "session": sid
        }

        try:
            response = requests.post(
                f"{self.base_url}/v1",
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            result = response.json()

            if result.get("status") == "ok":
                logger.info(f"Destroyed FlareSolverr session: {sid}")
                if sid == self._session_id:
                    self._session_id = None
                return True

        except Exception as e:
            logger.warning(f"Failed to destroy session {sid}: {e}")

        return False

    def list_sessions(self) -> list:
        """
        Получает список активных сессий.

        Returns:
            Список ID сессий
        """
        payload = {
            "cmd": "sessions.list"
        }

        try:
            response = requests.post(
                f"{self.base_url}/v1",
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            result = response.json()

            if result.get("status") == "ok":
                return result.get("sessions", [])

        except Exception as e:
            logger.warning(f"Failed to list sessions: {e}")

        return []

    def get_html(
        self,
        url: str,
        proxy: Optional[str] = None,
        max_timeout: Optional[int] = None
    ) -> str:
        """
        Удобный метод для получения HTML страницы.

        Args:
            url: URL страницы
            proxy: Прокси
            max_timeout: Таймаут

        Returns:
            HTML контент страницы
        """
        result = self.request_get(url, proxy, max_timeout)
        return result["solution"]["response"]

    def get_cookies(
        self,
        url: str,
        proxy: Optional[str] = None,
        max_timeout: Optional[int] = None
    ) -> list:
        """
        Получает cookies после решения Cloudflare challenge.

        Args:
            url: URL страницы
            proxy: Прокси
            max_timeout: Таймаут

        Returns:
            Список cookies
        """
        result = self.request_get(url, proxy, max_timeout)
        return result["solution"].get("cookies", [])

    def health_check(self, verbose: bool = False) -> bool:
        """
        Проверяет доступность FlareSolverr.

        Args:
            verbose: Если True, логирует дополнительную диагностику

        Returns:
            True если сервис доступен
        """
        try:
            response = requests.get(
                f"{self.base_url}/health",
                timeout=10
            )
            is_healthy = response.status_code == 200

            if verbose:
                logger.info(
                    f"FlareSolverr health check: "
                    f"url={self.base_url}, "
                    f"status={response.status_code}, "
                    f"healthy={is_healthy}"
                )

            return is_healthy
        except Exception as e:
            if verbose:
                logger.error(f"FlareSolverr health check failed: {e}")
            return False

    def get_version_info(self) -> Optional[Dict[str, Any]]:
        """
        Получает информацию о версии FlareSolverr.

        Returns:
            Dict с информацией о версии или None
        """
        try:
            response = requests.get(
                f"{self.base_url}/health",
                timeout=10
            )
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.warning(f"Failed to get FlareSolverr version: {e}")
        return None

    @property
    def session_id(self) -> Optional[str]:
        """Текущий ID сессии."""
        return self._session_id

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup session."""
        if self._session_id:
            self.destroy_session()
        return False
