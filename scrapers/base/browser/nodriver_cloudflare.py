"""
Nodriver Cloudflare Detection & Bypass
=======================================

Cloudflare challenge detection, bypass logic, and HTML extraction utilities
for use with nodriver-based browser automation.

Contains:
- Cloudflare challenge detection (is_cloudflare_blocked)
- Active bypass via CFVerify plugin and opencv template matching
- Passive wait fallback
- HTML extraction with multiple fallback strategies (Runtime.evaluate,
  CDP DOM.getOuterHTML, page.get_content)

These functions are designed to be called from NodriverBypass but are
standalone — they accept browser page objects and configuration as parameters.
"""

import asyncio
import logging
import random

from scrapers.base.browser.nodriver_stealth import (
    human_like_click,
    human_like_mouse_move,
    pre_click_behavior,
)

logger = logging.getLogger(__name__)


def is_cloudflare_blocked(html: str) -> bool:
    """
    Check if page contains Cloudflare challenge.

    Args:
        html: Page HTML content.

    Returns:
        True if Cloudflare challenge is detected, False otherwise.
    """
    if not html:
        return True

    cloudflare_indicators = [
        "just a moment",
        "checking your browser",
        "cf-browser-verification",
        "challenge-running",
        "ray id",
        "cf-turnstile",  # Class/ID of challenge UI element (removed after verification)
        "cf_chl_opt",
    ]

    html_lower = html.lower()
    return any(indicator in html_lower for indicator in cloudflare_indicators)


async def wait_for_cloudflare(
    page,
    *,
    use_cf_verify: bool = True,
    cf_verify_max_retries: int = 10,
    cf_verify_interval: float = 2.0,
    cloudflare_wait: float = 90.0,
) -> None:
    """
    Wait for Cloudflare challenge with active bypass attempt.

    Strategy:
    1. Use CFVerify plugin to find and click Turnstile iframe
    2. Fallback to opencv-based template matching with human-like behavior
    3. Passive wait as last resort

    Args:
        page: nodriver browser page/tab object.
        use_cf_verify: Whether to try CFVerify plugin first.
        cf_verify_max_retries: Max retries for cf-verify plugin.
        cf_verify_interval: Interval between cf-verify retries in seconds.
        cloudflare_wait: Maximum time for passive wait (seconds).
    """
    logger.debug("Attempting Cloudflare Turnstile bypass...")

    # First try CFVerify plugin (clicks on iframe - proven to work)
    if use_cf_verify:
        try:
            from scrapers.base.browser.nodriver_cf_verify import CFVerify
            cf_verify = CFVerify(_browser_tab=page, _debug=False)

            logger.debug("Using CFVerify plugin for Turnstile bypass...")
            result = await asyncio.wait_for(
                cf_verify.verify(
                    _max_retries=cf_verify_max_retries,
                    _interval_between_retries=cf_verify_interval,
                    _reload_page_after_n_retries=5  # Reload after 5 failed clicks for fresh challenge token
                ),
                timeout=cloudflare_wait
            )

            if result:
                logger.info("CFVerify successfully bypassed Cloudflare!")
                await asyncio.sleep(2)  # Wait for page to fully load
                return

            logger.warning("CFVerify returned False, trying opencv-based approach...")

        except asyncio.TimeoutError:
            logger.warning("CFVerify timed out, trying opencv-based approach...")
        except ImportError:
            logger.warning("CFVerify not available, trying opencv-based approach...")
        except Exception as e:
            logger.warning(f"CFVerify error: {e}, trying opencv-based approach...")

    # Fallback: try opencv template matching with human-like behavior (max 2 attempts)
    for attempt in range(min(2, cf_verify_max_retries)):
        try:
            # Check if already passed
            html = await page.get_content()
            if not is_cloudflare_blocked(html):
                logger.info("Cloudflare challenge already passed")
                return

            logger.debug(f"Opencv attempt {attempt + 1}/2: locating CF checkbox...")

            # Pre-click human behavior (scroll, mouse movement, thinking pause)
            await pre_click_behavior(page)

            # Find checkbox coordinates using opencv template matching
            coords = await asyncio.wait_for(
                page.template_location(), timeout=10
            )
            if not coords:
                logger.warning("CF checkbox not found in screenshot")
                await asyncio.sleep(cf_verify_interval)
                continue

            x, y = coords
            logger.debug(f"Found CF checkbox at ({x}, {y}), performing human-like click...")

            # Human-like behavior: move mouse to checkbox area with some randomness
            start_x = x + random.randint(-200, 200)
            start_y = y + random.randint(-100, 100)

            # Move mouse towards target in steps (simulates natural movement)
            await human_like_mouse_move(page, start_x, start_y, x, y)

            # Small random offset for click position
            click_x = x + random.randint(-3, 3)
            click_y = y + random.randint(-3, 3)

            # Human-like click with delay between press and release
            await human_like_click(page, click_x, click_y)

            # Wait for Cloudflare to process the click
            await asyncio.sleep(3 + random.uniform(0, 2))

            # Check if bypass succeeded
            html = await page.get_content()
            if not is_cloudflare_blocked(html):
                logger.info("Cloudflare bypassed with opencv human-like click!")
                return

        except asyncio.TimeoutError:
            logger.warning(f"Opencv template_location timed out on attempt {attempt + 1}")
        except Exception as e:
            logger.warning(f"Opencv attempt {attempt + 1} failed: {e}")

        await asyncio.sleep(cf_verify_interval)

    logger.warning("Active bypass methods exhausted, trying passive wait...")

    # Fallback: passive wait for Cloudflare challenge with early exit
    # Check every 3 seconds for faster detection of successful bypass
    logger.debug(f"Passive wait up to {cloudflare_wait}s for Cloudflare challenge (checking every 3s)...")

    check_interval = 3
    elapsed = 0

    while elapsed < cloudflare_wait:
        html = await page.get_content()

        if not is_cloudflare_blocked(html):
            logger.info(f"Cloudflare challenge passed after {elapsed}s (early exit)")
            return

        logger.debug(f"Cloudflare challenge in progress, waited {elapsed}s...")
        await asyncio.sleep(check_interval)
        elapsed += check_interval

    logger.warning(f"Cloudflare wait timeout after {elapsed}s")


async def get_html_via_cdp_dom(page, timeout: float = 30.0) -> str:
    """
    Get page HTML via CDP DOM.getOuterHTML — bypasses JS execution.

    This method works at the browser process level and does NOT require
    a functioning JS execution context. It's the reliable fallback when
    Runtime.evaluate hangs after Cloudflare bypass.

    Args:
        page: nodriver browser page/tab object.
        timeout: Timeout for each CDP call in seconds.

    Returns:
        Page HTML string, or empty string on failure.
    """
    try:
        import nodriver.cdp.dom as cdp_dom

        doc = await asyncio.wait_for(
            page.send(cdp_dom.get_document(-1, True)),
            timeout=timeout,
        )
        html = await asyncio.wait_for(
            page.send(
                cdp_dom.get_outer_html(backend_node_id=doc.backend_node_id)
            ),
            timeout=timeout,
        )
        return html if isinstance(html, str) else str(html) if html else ""
    except Exception as e:
        logger.warning(f"CDP DOM.getOuterHTML failed: {e}")
        return ""


async def get_html_with_fallback(page) -> str:
    """
    Get page HTML with fallback chain:
    1. Runtime.evaluate (fast path, 15s timeout)
    2. CDP DOM.getOuterHTML (reliable, no JS needed)
    3. page.get_content() (nodriver built-in)

    Args:
        page: nodriver browser page/tab object.

    Returns:
        Page HTML string.

    Raises:
        RuntimeError: If all methods fail.
    """
    # 1. Fast path: Runtime.evaluate
    try:
        logger.debug("Getting HTML via Runtime.evaluate (fast path)")
        result = await asyncio.wait_for(
            page.evaluate(
                "document.documentElement.outerHTML",
                return_by_value=True,
            ),
            timeout=15.0,
        )
        html = getattr(result, 'value', result) if hasattr(result, 'value') else result
        if not isinstance(html, str):
            html = str(html) if html else ""
        if html and len(html) >= 1000:
            logger.debug(f"Got {len(html)} bytes via Runtime.evaluate")
            return html
        logger.warning(f"Runtime.evaluate returned small content ({len(html)} bytes)")
    except asyncio.TimeoutError:
        logger.warning(
            "Runtime.evaluate timed out after 15s, "
            "falling back to CDP DOM.getOuterHTML"
        )
    except Exception as e:
        logger.warning(f"Runtime.evaluate failed: {e}, falling back to CDP DOM")

    # 2. CDP DOM.getOuterHTML — no JS execution needed
    html = await get_html_via_cdp_dom(page, timeout=30.0)
    if html and len(html) >= 1000:
        logger.debug(f"Got {len(html)} bytes via CDP DOM.getOuterHTML")
        return html

    # 3. Last resort: nodriver built-in get_content()
    try:
        logger.debug("Falling back to page.get_content()")
        html = await asyncio.wait_for(
            page.get_content(),
            timeout=30.0,
        )
        if html:
            logger.debug(f"Got {len(html)} bytes via page.get_content()")
            return html
    except Exception as e:
        logger.warning(f"page.get_content() failed: {e}")

    raise RuntimeError("All HTML extraction methods failed")


async def get_html_hung_runtime(page) -> str:
    """
    Get HTML when JS runtime is hung. Short timeouts, skip evaluate.

    When Runtime.evaluate hangs (common after CF bypass through slow proxies),
    this method uses only CDP DOM and get_content() with 10s timeouts instead
    of the normal 15s+30s+30s=75s fallback chain.

    Args:
        page: nodriver browser page/tab object.

    Returns:
        Page HTML string.

    Raises:
        RuntimeError: If all methods fail.
    """
    html = await get_html_via_cdp_dom(page, timeout=10.0)
    if html and len(html) >= 1000:
        logger.debug(f"[HUNG] Got {len(html)} bytes via CDP DOM")
        return html
    try:
        html = await asyncio.wait_for(page.get_content(), timeout=10.0)
        if html:
            logger.debug(f"[HUNG] Got {len(html)} bytes via get_content()")
            return html
    except Exception as e:
        logger.warning(f"[HUNG] get_content() failed: {e}")
    raise RuntimeError("All HTML extraction methods failed (runtime hung)")
