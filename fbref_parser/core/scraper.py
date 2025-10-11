"""
Web scraping utilities for FBref.com

This module provides functionality for fetching HTML content from FBref
and extracting tables using requests, BeautifulSoup, and pandas.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
from typing import List, Tuple

from ..constants import DEFAULT_HEADERS


class FBrefScraper:
    """
    Scraper for FBref.com football statistics

    Handles HTTP requests with proper headers and provides methods
    for extracting tables from HTML using pandas.read_html().
    """

    def __init__(self, headers=None):
        """
        Initialize the scraper

        Args:
            headers: Optional custom HTTP headers. Uses DEFAULT_HEADERS if not provided.
        """
        self.headers = headers or DEFAULT_HEADERS

    def fetch_page(self, url: str) -> requests.Response:
        """
        Fetch a page from FBref

        Args:
            url: The URL to fetch

        Returns:
            requests.Response object

        Raises:
            requests.HTTPError: If the request fails
        """
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
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
