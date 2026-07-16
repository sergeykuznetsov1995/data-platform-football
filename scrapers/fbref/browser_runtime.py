"""Immutable production contract for FBref's isolated browser stack."""

from pathlib import Path


CAMOUFOX_PACKAGE_VERSION = "0.4.11"
CAMOUFOX_BROWSER_VERSION = "152.0.4"
CAMOUFOX_BROWSER_RELEASE = "beta.26"
CAMOUFOX_FIREFOX_MAJOR = 152
PLAYWRIGHT_PACKAGE_VERSION = "1.59.0"
CURL_CFFI_PACKAGE_VERSION = "0.15.0"

# curl_cffi has no Firefox 152 preset yet. Its official guidance for skipped
# versions is to use the nearest previous fingerprint with current headers.
HTTP_IMPERSONATE_TARGET = "firefox147"

# FBref is isolated from SofaScore's reviewed v135 browser. Updating one source
# must never silently change the other's paid-canary runtime.
INSTALL_DIR = Path("/opt/fbref-camoufox")
EXECUTABLE_PATH = INSTALL_DIR / "camoufox-bin"


__all__ = [
    "CAMOUFOX_BROWSER_RELEASE",
    "CAMOUFOX_BROWSER_VERSION",
    "CAMOUFOX_FIREFOX_MAJOR",
    "CAMOUFOX_PACKAGE_VERSION",
    "CURL_CFFI_PACKAGE_VERSION",
    "EXECUTABLE_PATH",
    "HTTP_IMPERSONATE_TARGET",
    "INSTALL_DIR",
    "PLAYWRIGHT_PACKAGE_VERSION",
]
