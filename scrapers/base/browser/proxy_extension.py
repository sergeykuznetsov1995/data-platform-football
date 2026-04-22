"""
Proxy Authentication Extension
==============================

Creates Chrome extensions for proxy authentication.
Chrome doesn't support proxy authentication via command line,
so we create temporary extensions that handle auth.
"""

import json
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
var proxyConfig = {
    mode: "fixed_servers",
    rules: {
        singleProxy: {
            scheme: "http",
            host: """ + json.dumps(str(proxy_host)) + """,
            port: parseInt(""" + json.dumps(str(proxy_port)) + """)
        },
        bypassList: ["localhost"]
    }
};

var proxyAuth = {
    username: """ + json.dumps(str(proxy_user)) + """,
    password: """ + json.dumps(str(proxy_pass)) + """
};

chrome.proxy.settings.set({value: proxyConfig, scope: "regular"}, function() {});

function callbackFn(details) {
    return {
        authCredentials: {
            username: proxyAuth.username,
            password: proxyAuth.password
        }
    };
}

chrome.webRequest.onAuthRequired.addListener(
    callbackFn,
    {urls: ["<all_urls>"]},
    ['blocking']
);

function updateProxy(host, port, username, password) {
    proxyAuth.username = username;
    proxyAuth.password = password;
    proxyConfig.rules.singleProxy.host = host;
    proxyConfig.rules.singleProxy.port = parseInt(port);
    chrome.proxy.settings.set({value: proxyConfig, scope: "regular"}, function() {});
    return "ok";
}
"""

    # Create temp directory for extension with restricted permissions
    extension_dir = tempfile.mkdtemp(prefix='chrome_proxy_')
    os.chmod(extension_dir, 0o700)

    # Write manifest.json with restricted permissions
    manifest_path = os.path.join(extension_dir, 'manifest.json')
    with open(manifest_path, 'w') as f:
        f.write(manifest_json)
    os.chmod(manifest_path, 0o600)

    # Write background.js with restricted permissions (contains credentials)
    bg_path = os.path.join(extension_dir, 'background.js')
    with open(bg_path, 'w') as f:
        f.write(background_js)
    os.chmod(bg_path, 0o600)

    # Create zip file
    zip_path = os.path.join(extension_dir, 'proxy_auth.zip')
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.write(manifest_path, 'manifest.json')
        zf.write(bg_path, 'background.js')
    os.chmod(zip_path, 0o600)

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
