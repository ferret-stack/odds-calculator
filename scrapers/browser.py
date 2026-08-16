"""
Browser construction for the Selenium-backed scrapers.

Two things this fixes relative to the previous inline setup in
OddsCalculator.__init__:

1. It is lazy. The old code ran GeckoDriverManager().install() -- a network
   call -- inside __init__, so merely constructing an OddsCalculator required
   internet and a working driver CDN, even for runs that never scrape (the
   nightly pipeline, the validation tools, the tests). Driver resolution now
   happens on first use.

2. It is not Firefox-only. The scraper is run locally today, but nothing in it
   is local-specific, and a VM image is more likely to carry Chromium than
   Firefox. Engine is chosen by ODDS_BROWSER (firefox|chrome), defaulting to
   firefox to match the current local setup, so today's behaviour is unchanged.

Environment overrides, all optional:
    ODDS_BROWSER         firefox (default) | chrome
    ODDS_DRIVER_PATH     explicit driver binary, skips the download manager
    ODDS_BROWSER_BINARY  explicit browser binary
    ODDS_HEADLESS        0 to run headed (default headless)
"""

import os
import threading


class BrowserUnavailable(RuntimeError):
    """Raised when no usable browser/driver could be resolved."""


_service_cache = {}
_lock = threading.Lock()


def _headless():
    return os.environ.get('ODDS_HEADLESS', '1') not in ('0', 'false', 'False')


def _resolve_service(engine):
    """Build (and memoise) the driver Service for the chosen engine."""
    with _lock:
        if engine in _service_cache:
            return _service_cache[engine]

        explicit = os.environ.get('ODDS_DRIVER_PATH')

        if engine == 'chrome':
            from selenium.webdriver.chrome.service import Service as ChromeService
            if explicit:
                service = ChromeService(explicit)
            else:
                # Selenium 4.6+ resolves the driver itself via Selenium Manager.
                service = ChromeService()
        else:
            from selenium.webdriver.firefox.service import Service as FirefoxService
            if explicit:
                service = FirefoxService(explicit)
            else:
                try:
                    from webdriver_manager.firefox import GeckoDriverManager
                    service = FirefoxService(GeckoDriverManager().install())
                except Exception:
                    # Fall back to Selenium Manager / PATH lookup.
                    service = FirefoxService()

        _service_cache[engine] = service
        return service


def make_driver(engine=None):
    """
    Return a configured WebDriver. Caller owns it and must call .quit().

    Raises BrowserUnavailable rather than a raw WebDriverException so callers
    can distinguish "no browser on this machine" from "the page did not parse".
    """
    engine = (engine or os.environ.get('ODDS_BROWSER', 'firefox')).lower()
    binary = os.environ.get('ODDS_BROWSER_BINARY')

    try:
        from selenium import webdriver

        if engine == 'chrome':
            from selenium.webdriver.chrome.options import Options
            options = Options()
            if _headless():
                options.add_argument('--headless=new')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--window-size=1920,1080')
            if binary:
                options.binary_location = binary
            return webdriver.Chrome(service=_resolve_service(engine), options=options)

        from selenium.webdriver.firefox.options import Options
        options = Options()
        if _headless():
            options.add_argument('--headless')
        options.add_argument('--width=1920')
        options.add_argument('--height=1080')
        if binary:
            options.binary_location = binary
        return webdriver.Firefox(service=_resolve_service(engine), options=options)

    except Exception as exc:
        raise BrowserUnavailable(
            f'could not start a {engine} WebDriver: {type(exc).__name__}: {exc}'
        ) from exc


def accept_cookies(driver, timeout=10):
    """Dismiss the OneTrust banner if it is showing. Never raises."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    for locator in (
        (By.ID, 'onetrust-accept-btn-handler'),
        (By.XPATH, "//button[contains(., 'Accept All Cookies')]"),
        (By.XPATH, "//button[contains(., 'Accept')]"),
    ):
        try:
            WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable(locator)).click()
            return True
        except Exception:
            timeout = 2  # only wait the full timeout on the first attempt
    return False
