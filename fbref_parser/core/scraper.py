"""
Web scraping utilities for FBref.com

This module provides functionality for fetching HTML content from FBref
and extracting tables using cloudscraper, BeautifulSoup, and pandas.

Includes rate limiting and retry logic to comply with FBref's requirements.
"""

import pandas as pd
import cloudscraper
import time
import random
import logging
from bs4 import BeautifulSoup
from io import StringIO
from typing import List, Tuple
from collections import deque
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

from ..constants import DEFAULT_HEADERS, MIN_REQUEST_DELAY, MAX_REQUEST_DELAY, USER_AGENT_POOL, MAX_REQUESTS_PER_MINUTE

# Configure logging
logger = logging.getLogger(__name__)


class FBrefScraper:
    """
    Scraper for FBref.com football statistics

    Uses cloudscraper to bypass Cloudflare protection and implements
    strict rate limiting (6-8 seconds between requests, max 10 requests/minute)
    to comply with FBref's policy. Includes Tenacity-based retry logic with
    exponential backoff for handling 429 errors and network failures.

    Features:
    - Request rate tracking with sliding 60-second window
    - User-Agent rotation from pool of modern browsers
    - Automatic enforcement of 10 requests/minute limit
    - Exponential backoff retry (up to 5 attempts)
    - Adaptive delay with jitter (10-20%)
    - Proper handling of Retry-After header
    """

    def __init__(self, headers=None):
        """
        Initialize the scraper with cloudscraper session

        Args:
            headers: Optional custom HTTP headers. Uses DEFAULT_HEADERS if not provided.
        """
        # Create cloudscraper session with enhanced browser emulation
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            },
            # Enable cookie handling for session persistence
            allow_brotli=True,
            # Delay parameter helps with JS challenge solving
            delay=10
        )

        # Update session headers with random User-Agent
        headers = headers or DEFAULT_HEADERS.copy()
        headers['User-Agent'] = self.get_random_user_agent()
        self.scraper.headers.update(headers)

        # Track request count for User-Agent rotation
        self.request_count = 0

        # Request rate tracker: sliding 60-second window (max 10 requests)
        # Stores timestamps of recent requests using deque for efficient FIFO operations
        self.request_times = deque(maxlen=MAX_REQUESTS_PER_MINUTE)

        # Track last visited URL for Referer header
        self.last_url = "https://fbref.com/"

    def get_random_user_agent(self) -> str:
        """
        Get a random User-Agent from the pool

        Returns:
            Random User-Agent string
        """
        return random.choice(USER_AGENT_POOL)

    def update_user_agent(self):
        """
        Update the session's User-Agent header with a new random value

        This helps avoid 403 errors by making requests appear more human-like
        """
        new_ua = self.get_random_user_agent()
        self.scraper.headers.update({'User-Agent': new_ua})
        logger.info(f"Rotated User-Agent: {new_ua[:50]}...")
        print(f"🔄 Rotated User-Agent: {new_ua[:50]}...")

    def enforce_rate_limit(self):
        """
        Enforce FBref's 10 requests per minute limit using sliding window.

        Removes request timestamps older than 60 seconds from the tracking deque.
        If 10 requests have been made in the last 60 seconds, waits until the
        oldest request is outside the window (plus 2 second safety buffer).

        This prevents hitting FBref's rate limit and avoids 24-hour IP bans.
        """
        now = time.time()

        # Remove requests older than 60 seconds from tracking
        while self.request_times and self.request_times[0] < now - 60:
            self.request_times.popleft()

        # If we've hit the limit (10 requests in last 60s), wait
        if len(self.request_times) >= MAX_REQUESTS_PER_MINUTE:
            # Calculate wait time: time until oldest request is >60s old + safety buffer
            wait_time = 60 - (now - self.request_times[0]) + 2
            logger.warning(f"Rate limit reached ({MAX_REQUESTS_PER_MINUTE} req/min). Waiting {wait_time:.1f}s...")
            print(f"⏸️  Rate limit: {MAX_REQUESTS_PER_MINUTE} requests in 60s. Waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
            # Clear old requests after waiting
            self.request_times.clear()

    def adaptive_delay(self) -> float:
        """
        Calculate adaptive delay with jitter to avoid detection patterns.

        Returns base delay (6-8 seconds) plus random jitter (10-20% of base).
        This makes request timing less predictable and more human-like.

        Returns:
            Total delay in seconds (typically 6.6-9.6 seconds)
        """
        base_delay = random.uniform(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY)
        jitter_percent = random.uniform(0.1, 0.2)  # 10-20% jitter
        jitter = base_delay * jitter_percent
        total_delay = base_delay + jitter
        return total_delay

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=60),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((
            Exception,  # Catch all exceptions for retry
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def fetch_page(self, url: str):
        """
        Fetch a page from FBref with comprehensive rate limiting and retry logic.

        Implements (via Tenacity decorator + method logic):
        - Sliding window rate limiting: Max 10 requests per 60 seconds
        - Adaptive delay: 6-8 seconds base + 10-20% jitter between requests
        - User-Agent rotation: Random rotation every 3-5 requests
        - Exponential backoff retry: Up to 5 attempts (4s, 8s, 16s, 32s, 60s max)
        - 429 error handling: Respects Retry-After header from server
        - 403 error handling: Rotates User-Agent and retries
        - Referer header: Sets proper referrer for legitimate browsing simulation

        Args:
            url: The URL to fetch

        Returns:
            Response object from cloudscraper

        Raises:
            Exception: If all retry attempts fail (after 5 attempts)

        Note:
            The @retry decorator provides exponential backoff. This method
            handles rate limiting and 429/403 errors, then raises exceptions
            to trigger Tenacity's retry mechanism.
        """
        # Enforce 10 requests/minute limit before making request
        self.enforce_rate_limit()

        # Rotate User-Agent every 3-5 requests (helps avoid 403)
        if self.request_count > 0 and self.request_count % random.randint(3, 5) == 0:
            self.update_user_agent()

        # Apply adaptive delay with jitter before request
        delay = self.adaptive_delay()
        logger.debug(f"Waiting {delay:.1f}s before request (rate limiting + jitter)...")
        print(f"⏳ Waiting {delay:.1f}s before request (rate limiting + jitter)...")
        time.sleep(delay)

        # Update Referer header to simulate natural browsing
        # First request gets homepage referer, subsequent get previous URL
        request_headers = self.scraper.headers.copy()
        if self.last_url:
            request_headers['Referer'] = self.last_url
            # Update Sec-Fetch-Site for same-origin navigation
            request_headers['Sec-Fetch-Site'] = 'same-origin'

        # Make request with cloudscraper (handles Cloudflare automatically)
        logger.info(f"Fetching: {url[:80]}...")
        print(f"🌐 Fetching: {url[:80]}...")
        response = self.scraper.get(url, headers=request_headers, timeout=30)

        # Register request timestamp for rate limiting
        self.request_times.append(time.time())

        # Increment request counter for User-Agent rotation tracking
        self.request_count += 1

        # Handle 403 (Forbidden) - often indicates bot detection
        if response.status_code == 403:
            logger.warning("Received 403 Forbidden. Implementing advanced anti-bot bypass...")
            print(f"⚠️  Forbidden (403). Trying advanced anti-bot measures...")

            # Multi-step recovery strategy
            # Step 1: Rotate User-Agent
            self.update_user_agent()

            # Step 2: Clear cookies and restart session (simulates new browser session)
            self.scraper.cookies.clear()

            # Step 3: Visit homepage first (establish legitimate session)
            print(f"🏠 Visiting FBref homepage to establish session...")
            time.sleep(random.uniform(3, 5))
            try:
                homepage_response = self.scraper.get("https://fbref.com/", timeout=30)
                if homepage_response.status_code == 200:
                    print(f"✅ Homepage visited successfully")
                    time.sleep(random.uniform(2, 4))
            except Exception as e:
                logger.warning(f"Homepage visit failed: {e}")

            # Step 4: Longer wait (90-120 seconds) before retry
            wait_time = random.uniform(90, 120)
            print(f"⏱️  Waiting {wait_time:.1f}s before retry...")
            time.sleep(wait_time)

            # Raise exception to trigger Tenacity retry
            raise Exception("403 Forbidden - Applied advanced bypass, retrying")

        # Handle 429 (Too Many Requests) - rate limit exceeded
        if response.status_code == 429:
            # Check for Retry-After header (tells us how long to wait)
            retry_after = int(response.headers.get('Retry-After', 300))
            logger.warning(f"Received 429 Rate Limited. Retry-After: {retry_after}s")
            print(f"⚠️  Rate limited (429). Waiting {retry_after}s as requested by server...")
            time.sleep(retry_after)
            # Raise exception to trigger Tenacity retry
            raise Exception(f"429 Too Many Requests - waiting {retry_after}s")

        # Raise for other HTTP errors (4xx, 5xx) to trigger retry
        response.raise_for_status()

        # Success! Update last URL for next request's Referer header
        self.last_url = url
        logger.info(f"Successfully fetched: {url[:80]}...")
        print(f"✅ Successfully fetched: {url[:80]}...")
        return response

    def get_soup(self, url: str) -> BeautifulSoup:
        """
        Get BeautifulSoup object from URL

        Args:
            url: The URL to fetch and parse

        Returns:
            BeautifulSoup object
        """
        response = self.fetch_page(url)
        return BeautifulSoup(response.content, 'html.parser')


def extract_all_tables(html_content: str, header_levels: List[int] = None) -> List[pd.DataFrame]:
    """
    Extract all tables from HTML content using pandas.read_html()

    Args:
        html_content: HTML content as string
        header_levels: List of header row indices (default: [0, 1] for MultiIndex)

    Returns:
        List of pandas DataFrames, one for each table found
    """
    if header_levels is None:
        header_levels = [0, 1]

    try:
        tables = pd.read_html(StringIO(html_content), header=header_levels)
        return tables
    except Exception as e:
        print(f"Error extracting tables: {e}")
        return []
