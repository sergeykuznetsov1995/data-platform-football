"""
Unit tests for DriverFactory Chrome options.

Verifies CF invariants (#567): the Selenium driver paths must NOT set
GPU-disabling flags that null the WebGL context Cloudflare uses to verify a
"real" browser. Mirrors the drissionpage/nodriver fixes (#469 / PR #566).
"""

from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.unit
class TestDriverFactoryCFInvariants:
    """DriverFactory must not emit Cloudflare-violating GPU flags (#567)."""

    def test_undetected_driver_omits_cf_violating_gpu_flags(self):
        """_create_undetected_driver must not set --disable-gpu /
        --disable-software-rasterizer (null WebGL = CF bot marker)."""
        # Arrange
        from scrapers.base.browser.driver_factory import (
            BrowserConfig,
            DriverFactory,
        )

        captured = []
        mock_options = MagicMock()
        mock_options.add_argument.side_effect = lambda arg: captured.append(arg)
        mock_uc = MagicMock()
        mock_uc.ChromeOptions.return_value = mock_options

        factory = DriverFactory(BrowserConfig())

        # Act
        with patch(
            "scrapers.base.browser.driver_factory.uc", mock_uc
        ), patch(
            "scrapers.base.browser.driver_factory.find_chrome_binary",
            return_value="/usr/bin/chromium",
        ), patch.object(
            factory, "_detect_chrome_version", return_value=120
        ), patch.object(
            factory, "_find_chromedriver", return_value="/usr/bin/chromedriver"
        ):
            factory._create_undetected_driver()

        # Assert
        assert "--disable-gpu" not in captured
        assert "--disable-software-rasterizer" not in captured
        # Sanity: options were actually built (guards against a no-op test).
        assert "--no-sandbox" in captured

    def test_standard_driver_omits_cf_violating_gpu_flag(self):
        """_create_standard_driver (fallback) must not set --disable-gpu."""
        # Arrange
        from scrapers.base.browser.driver_factory import (
            BrowserConfig,
            DriverFactory,
        )

        factory = DriverFactory(BrowserConfig())

        # Act
        with patch("selenium.webdriver.Chrome") as mock_chrome:
            factory._create_standard_driver()
            options = mock_chrome.call_args.kwargs["options"]

        # Assert
        assert "--disable-gpu" not in options.arguments
        # Sanity: options were actually built (guards against a no-op test).
        assert "--no-sandbox" in options.arguments
