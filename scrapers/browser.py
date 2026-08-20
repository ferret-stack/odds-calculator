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
    ODDS_BROWSER              firefox (default) | chrome
    ODDS_DRIVER_PATH          explicit driver binary, skips the download manager
    ODDS_BROWSER_BINARY       explicit browser binary
    ODDS_HEADLESS             0 to run headed (default headless) -- run this
                              way to watch a page live and inspect an element
                              that a scraper is failing to find
    ODDS_COOKIE_ACCEPT_XPATH  XPath for the cookie-consent accept button,
                              when the built-in guesses in accept_cookies()
                              do not match the button actually on the page
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


# Common consent-banner phrases across CMPs (OneTrust, Cookiebot, Sourcepoint,
# a bespoke implementation). Tried most-specific first, since the first
# clickable match wins and a bare "accept" could in principle match before a
# more precise "accept all cookies" if order were reversed.
_COOKIE_PHRASES = (
    'accept all cookies', 'accept all', 'i accept', 'accept',
    'allow all cookies', 'allow all', 'agree to all', 'i agree', 'agree',
    'got it',
)

_LOWER = 'abcdefghijklmnopqrstuvwxyz'
_UPPER = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'


def _phrase_condition(phrase):
    """The case-insensitive XPath predicate for one consent phrase."""
    return (f"contains(translate(normalize-space(.), '{_UPPER}', '{_LOWER}'), "
            f"'{phrase}')")


def cookie_accept_xpaths(phrases=_COOKIE_PHRASES):
    """
    The ordered list of phrase-derived XPath expressions accept_cookies()
    tries, after the override and the OneTrust id (both handled by the
    caller -- neither is phrase-derived).

    Pure and Selenium-free (`By` values are attached by the caller) so the
    locator set itself -- phrase coverage, element scope, ordering -- can be
    tested directly, without needing a browser to drive it.
    """
    xpaths = []
    for phrase in phrases:
        cond = _phrase_condition(phrase)
        xpaths.append(f"//button[{cond}] | //a[{cond}] | //*[@role='button'][{cond}]")
    return xpaths


def cookie_banner_present(driver):
    """
    Instant, no-wait check for whether a consent banner is currently showing.

    Used to decide whether a defensive re-check of accept_cookies() is worth
    running (e.g. right before clicking a tab that might trigger a
    client-side re-render and bring the banner back) without paying that
    check's full timeout budget on every single call when there is nothing
    to dismiss, which is the common case.
    """
    from selenium.webdriver.common.by import By

    for xp in cookie_accept_xpaths():
        try:
            if driver.find_elements(By.XPATH, xp):
                return True
        except Exception:
            pass
    return False


def cookie_locator_specs(override=None):
    """
    The ordered list of (kind, value) locator specs accept_cookies() tries.

    kind is 'xpath' or 'id'. Pure and Selenium-free -- `By` is attached by
    the caller -- so the PRIORITY ORDER is directly testable. That order is
    exactly what was wrong before: the OneTrust id went first and got the
    full timeout, despite never once matching in three live reports, leaving
    almost none of the budget for the phrase-based locator that does match.
    The phrase locators go first now; OneTrust is a last-resort fallback.
    """
    specs = []
    if override:
        specs.append(('xpath', override))
    specs += [('xpath', xp) for xp in cookie_accept_xpaths()]
    specs.append(('id', 'onetrust-accept-btn-handler'))
    return specs


def accept_cookies(driver, timeout=10):
    """
    Dismiss the cookie consent banner if it is showing.

    Never raises -- a missing or already-dismissed banner is not an error.

    Tried in order:
      1. ODDS_COOKIE_ACCEPT_XPATH, if set -- for when a human has identified
         the real button by hand (inspect it with ODDS_HEADLESS=0) and wants
         to unblock a run immediately without a code change.
      2. The phrase-based locators -- confirmed against a real page to be
         what actually matches on this site ("Accept All Cookies").
      3. The classic OneTrust id, LAST. It has never matched in three live
         reports, which means this site is not classic OneTrust -- trying it
         first, as an earlier version did, spent the entire timeout budget on
         a guess that never pays off and left almost nothing for the locator
         that does. Only the FIRST locator tried gets the full `timeout`;
         every locator after it gets a much shorter one, so locator order is
         a priority order, not just a list.

    Prints when it could NOT dismiss the banner, including which kind of
    failure it was (nothing matched vs. something matched but the click
    was blocked) -- a banner left standing is a plausible cause of a later
    "element not found" or "click intercepted" failure elsewhere on the page
    (the tab it covers becomes unclickable, for instance), and staying silent
    here would hide that as the real cause of a failure that looks unrelated.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import ElementClickInterceptedException

    specs = cookie_locator_specs(override=os.environ.get('ODDS_COOKIE_ACCEPT_XPATH'))
    locators = [(By.XPATH, v) if kind == 'xpath' else (By.ID, v)
                for kind, v in specs]

    any_intercepted = False

    for locator in locators:
        try:
            WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable(locator)).click()
            return True
        except ElementClickInterceptedException:
            # The element was found and matched -- something else is on top
            # of it. More timeout will not fix that; worth saying so
            # separately from "nothing matched at all".
            any_intercepted = True
        except Exception:
            pass
        timeout = 1  # only the first locator tried gets the full budget

    if any_intercepted:
        print('  ⚠ could not dismiss the cookie banner -- a matching accept '
              'button was found but the click was intercepted, meaning '
              'something else is covering it (another overlay, most likely).')
    else:
        print('  ⚠ could not dismiss the cookie banner -- none of the known '
              'accept-button patterns matched. If it is still showing under '
              'ODDS_HEADLESS=0, inspect the real button and set '
              'ODDS_COOKIE_ACCEPT_XPATH to an XPath for it.')
    return False
