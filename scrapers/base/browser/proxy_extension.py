"""
Proxy Authentication Extension
==============================

Creates Chrome extensions for proxy authentication.
Chrome doesn't support proxy authentication via command line,
so we create temporary extensions that handle auth.
"""

import os
import tempfile
import zipfile
from urllib.parse import urlparse


def create_proxy_auth_extension(
    proxy_host: str,
    proxy_port: int,
    proxy_user: str,
    proxy_pass: str
) -> str:
    """
    Create a Chrome extension for proxy authentication.

    Chrome doesn't support proxy authentication via command line,
    so we create a temporary extension that handles auth.

    Args:
        proxy_host: Proxy hostname
        proxy_port: Proxy port
        proxy_user: Proxy username
        proxy_pass: Proxy password

    Returns:
        Path to the created extension zip file
    """
    manifest_json = """
{
    "version": "1.0.0",
    "manifest_version": 2,
    "name": "Chrome Proxy Auth",
    "permissions": [
        "proxy",
        "tabs",
        "unlimitedStorage",
        "storage",
        "<all_urls>",
        "webRequest",
        "webRequestBlocking"
    ],
    "background": {
        "scripts": ["background.js"]
    },
    "minimum_chrome_version":"22.0.0"
}
"""

    background_js = """
var config = {
    mode: "fixed_servers",
    rules: {
        singleProxy: {
            scheme: "http",
            host: "%s",
            port: parseInt(%s)
        },
        bypassList: ["localhost"]
    }
};

chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

function callbackFn(details) {
    return {
        authCredentials: {
            username: "%s",
            password: "%s"
        }
    };
}

chrome.webRequest.onAuthRequired.addListener(
    callbackFn,
    {urls: ["<all_urls>"]},
    ['blocking']
);
""" % (proxy_host, proxy_port, proxy_user, proxy_pass)

    # Create temp directory for extension
    extension_dir = tempfile.mkdtemp(prefix='chrome_proxy_')

    # Write manifest.json
    with open(os.path.join(extension_dir, 'manifest.json'), 'w') as f:
        f.write(manifest_json)

    # Write background.js
    with open(os.path.join(extension_dir, 'background.js'), 'w') as f:
        f.write(background_js)

    # Create zip file
    zip_path = os.path.join(extension_dir, 'proxy_auth.zip')
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.write(os.path.join(extension_dir, 'manifest.json'), 'manifest.json')
        zf.write(os.path.join(extension_dir, 'background.js'), 'background.js')

    return zip_path


def parse_proxy_url(proxy_url: str) -> dict:
    """
    Parse proxy URL into components.

    Args:
        proxy_url: Proxy URL like http://user:pass@host:port

    Returns:
        Dict with host, port, username, password, scheme
    """
    parsed = urlparse(proxy_url)
    return {
        'host': parsed.hostname or '',
        'port': parsed.port or 8080,
        'username': parsed.username,
        'password': parsed.password,
        'scheme': parsed.scheme or 'http',
    }
