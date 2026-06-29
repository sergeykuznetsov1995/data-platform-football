"""
Nodriver Stealth Utilities
===========================

Stealth JavaScript injection and human-like behavior simulation for
evading Cloudflare bot detection.

Contains:
- Stealth JS patches (navigator.webdriver, plugins, languages, etc.)
- Window size and User-Agent rotation constants
- Human-like mouse movement, clicking, and pre-click behavior

These functions are designed to be called from NodriverBypass but are
standalone — they accept a browser page object as a parameter.
"""

import asyncio
import logging
import random

logger = logging.getLogger(__name__)


# Stealth JS script injected via CDP Page.addScriptToEvaluateOnNewDocument.
# Executes BEFORE any page scripts on every navigation.
# Based on puppeteer-extra-plugin-stealth patches.
STEALTH_JS = """
    // --- navigator.webdriver ---
    // Double insurance on top of nodriver's built-in patch
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true
    });

    // --- navigator.plugins ---
    // Headless Chrome has empty plugins array — dead giveaway
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const plugins = [
                { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
                { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
            ];
            plugins.length = 3;
            return plugins;
        },
        configurable: true
    });

    // --- navigator.languages ---
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
        configurable: true
    });

    // --- navigator.permissions.query ---
    // Headless returns 'denied' for notifications; real browsers return 'prompt'
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : originalQuery(parameters)
    );

    // --- window.chrome ---
    // Must exist with runtime property in real Chrome
    if (!window.chrome) {
        window.chrome = {};
    }
    if (!window.chrome.runtime) {
        window.chrome.runtime = {};
    }

    // --- Notification.permission ---
    if (Notification.permission === 'denied') {
        Object.defineProperty(Notification, 'permission', {
            get: () => 'default',
            configurable: true
        });
    }

    """

# Window sizes for rotation (non-standard to avoid fingerprinting)
WINDOW_SIZES = [
    (1366, 768),   # Common laptop
    (1536, 864),   # Scaled laptop
    (1440, 900),   # MacBook
    (1280, 800),   # Smaller laptop
    (1600, 900),   # Wide laptop
]

# User agents for rotation (Chrome 131-133, актуальные для Q1 2026)
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
]


async def inject_stealth_js(page) -> None:
    """
    Inject stealth JavaScript via CDP Page.addScriptToEvaluateOnNewDocument.

    This ensures the stealth patches run BEFORE any page scripts on every
    navigation, hiding automation markers from Cloudflare Turnstile.

    Args:
        page: nodriver browser page/tab object.
    """
    try:
        import nodriver.cdp.page as cdp_page

        await page.send(
            cdp_page.add_script_to_evaluate_on_new_document(source=STEALTH_JS)
        )
        logger.info("Stealth JS injected via addScriptToEvaluateOnNewDocument")
    except Exception as e:
        logger.warning(f"Failed to inject stealth JS: {e}")


async def pre_click_behavior(page) -> None:
    """
    Human-like behavior before clicking the Cloudflare checkbox.

    Simulates natural user behavior: random scroll, mouse movement, thinking pause.
    This makes the browser session appear more human-like to Cloudflare.

    Args:
        page: nodriver browser page/tab object.
    """
    import nodriver.cdp.input_ as cdp_input

    # Random scroll (40% chance)
    if random.random() < 0.4:
        scroll_y = random.randint(-50, 100)
        await page.evaluate(f'window.scrollBy(0, {scroll_y})')
        await asyncio.sleep(random.uniform(0.3, 0.8))
        logger.debug(f"Pre-click: scrolled {scroll_y}px")

    # Random mouse movement (50% chance)
    if random.random() < 0.5:
        rand_x = random.randint(100, 800)
        rand_y = random.randint(100, 400)
        await page.send(
            cdp_input.dispatch_mouse_event(
                type_="mouseMoved",
                x=rand_x,
                y=rand_y
            )
        )
        await asyncio.sleep(random.uniform(0.5, 1.5))
        logger.debug(f"Pre-click: mouse moved to ({rand_x}, {rand_y})")

    # "Thinking" pause (always)
    thinking_time = random.uniform(1.0, 3.0)
    await asyncio.sleep(thinking_time)
    logger.debug(f"Pre-click: thinking pause {thinking_time:.1f}s")


async def human_like_mouse_move(
    page, start_x: int, start_y: int, end_x: int, end_y: int
) -> None:
    """
    Move mouse from start to end position in a human-like manner.

    Uses bezier-like movement with random delays to simulate natural mouse movement.

    Args:
        page: nodriver browser page/tab object.
        start_x: Starting X coordinate.
        start_y: Starting Y coordinate.
        end_x: Target X coordinate.
        end_y: Target Y coordinate.
    """
    import nodriver.cdp.input_ as cdp_input

    steps = random.randint(5, 10)
    for i in range(steps + 1):
        # Progress from 0 to 1
        t = i / steps
        # Add slight curve using easing function
        eased_t = t * t * (3 - 2 * t)  # smoothstep

        # Current position with some random jitter
        curr_x = start_x + (end_x - start_x) * eased_t + random.uniform(-2, 2)
        curr_y = start_y + (end_y - start_y) * eased_t + random.uniform(-2, 2)

        await page.send(
            cdp_input.dispatch_mouse_event(
                type_="mouseMoved",
                x=curr_x,
                y=curr_y
            )
        )
        # Variable delay between movements (50-150ms)
        await asyncio.sleep(random.uniform(0.05, 0.15))


async def human_like_click(page, x: int, y: int) -> None:
    """
    Perform a human-like mouse click with realistic timing.

    Humans have ~100-300ms delay between press and release.

    Args:
        page: nodriver browser page/tab object.
        x: Click X coordinate.
        y: Click Y coordinate.
    """
    import nodriver.cdp.input_ as cdp_input

    # Mouse press
    await page.send(
        cdp_input.dispatch_mouse_event(
            type_="mousePressed",
            x=x,
            y=y,
            button=cdp_input.MouseButton("left"),
            buttons=1,
            click_count=1
        )
    )

    # Human-like delay between press and release (100-300ms)
    await asyncio.sleep(random.uniform(0.1, 0.3))

    # Mouse release
    await page.send(
        cdp_input.dispatch_mouse_event(
            type_="mouseReleased",
            x=x,
            y=y,
            button=cdp_input.MouseButton("left"),
            buttons=1,
            click_count=1
        )
    )

    logger.debug(f"Human-like click at ({x}, {y}) completed")
