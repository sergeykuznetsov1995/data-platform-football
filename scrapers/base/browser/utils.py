"""
Browser Utilities
=================

Utility functions for browser automation.
"""

import os
import shutil


def find_chrome_binary() -> str:
    """
    Find Chrome/Chromium binary path.

    Checks:
    1. CHROME_BIN environment variable
    2. Common installation paths
    3. PATH via shutil.which

    Returns:
        Path to Chrome/Chromium binary

    Raises:
        FileNotFoundError: If Chrome/Chromium is not found
    """
    # Check environment variable first
    env_chrome = os.environ.get('CHROME_BIN')
    if env_chrome and os.path.isfile(env_chrome):
        return env_chrome

    # Check common paths
    common_paths = [
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
        '/usr/bin/google-chrome',
        '/usr/bin/google-chrome-stable',
    ]

    for path in common_paths:
        if os.path.isfile(path):
            return path

    # Try shutil.which as fallback
    commands = ['chromium', 'chromium-browser', 'google-chrome']
    for cmd in commands:
        found = shutil.which(cmd)
        if found:
            return found

    raise FileNotFoundError(
        "Chrome/Chromium not found. Set CHROME_BIN or install chromium."
    )
