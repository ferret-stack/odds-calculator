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



def cookie_locator_specs(override=None):
    """
    The ordered list of (kind, value) locator specs accept_cookies() tries.

    kind is 'xpath' or 'id'. Pure and Selenium-free -- `By` is attached by
    the caller -- so the PRIORITY ORDER is directly testable. Order is
    specificity: the override a human set by hand first, then the phrase
    locators, then the classic OneTrust id, which has never once matched on
    this site. Every locator is now re-checked on each poll rather than
    consuming a slice of the timeout budget in turn, so order decides which
    match WINS, not which one gets time to be found.
    """
    specs = []
    if override:
        specs.append(('xpath', override))
    specs += [('xpath', xp) for xp in cookie_accept_xpaths()]
    specs.append(('id', 'onetrust-accept-btn-handler'))
    return specs


# ---------------------------------------------------------------------------
# Was the click delivered, and did it change anything?
# ---------------------------------------------------------------------------
#
# THE FAILURE THIS SECTION EXISTS FOR
#
# accept_cookies() reported success -- a locator matched, WebDriver's .click()
# returned without raising, no warning was printed -- and the banner stayed on
# screen for the entire run, still covering the Stats tab, which then failed
# with ElementClickInterceptedException.
#
# The old success criterion was "the .click() call did not raise". That is not
# evidence that anything happened. WebDriver raises on a click it could not
# DELIVER (intercepted, stale, not interactable). It does not raise on a click
# it delivered to an element that ignored it. Three ordinary page conditions
# produce exactly that, and all three are indistinguishable from the console:
#
#   * a decoy match -- a duplicate of the banner left in the DOM (a
#     mobile/desktop pair, or a pre-animation copy) at opacity:0 or zero size.
#     is_displayed() reports opacity:0 elements as DISPLAYED, and
#     EC.element_to_be_clickable() checks only displayed+enabled, so a decoy
#     that precedes the real button in document order is the one found,
#     clicked, and reported as a success. Note that find_element() returns the
#     first match in DOCUMENT order, not in the order the union arms of the
#     XPath are written, so which copy wins is decided by the page's markup.
#   * an unhydrated button -- server-rendered markup whose consent handler has
#     not been attached yet. The click lands on the right element; there is
#     simply no listener on it yet, and only a LATER click does anything.
#   * an occluded button -- something transparent on top of it. The click goes
#     to the overlay, which is not a button, so nothing raises.
#
# So the criterion is replaced. Click, then ask the PAGE whether the banner is
# actually gone, and when it is not, say which of the three it was. Locator
# coverage and locator priority (the two previous rounds of this bug) are
# unchanged and are not what this addresses.

# Selenium's By values are plain W3C strings. Spelling them out, preferring the
# real import when it is available, is what lets everything below be driven by
# a fake driver in the tests with no selenium and no browser -- the only way
# this failure can be reproduced at all, since the site is unreachable from
# where the fix is written.
try:
    from selenium.webdriver.common.by import By as _By
    BY_XPATH, BY_ID = _By.XPATH, _By.ID
except Exception:
    BY_XPATH, BY_ID = 'xpath', 'id'


# Measures one candidate the way the browser will when the click is dispatched:
# its real box, its computed style, and -- the part that matters -- which
# element is actually on top at the point the click will be aimed at.
_TARGET_PROBE_JS = """
var el = arguments[0];
var rect = el.getBoundingClientRect();
var style = window.getComputedStyle(el);
var cx = rect.left + rect.width / 2;
var cy = rect.top + rect.height / 2;
function describe(node) {
    if (!node || !node.tagName) { return null; }
    var out = node.tagName.toLowerCase();
    if (node.id) { out += '#' + node.id; }
    var cls = node.getAttribute ? node.getAttribute('class') : null;
    if (cls) { out += '.' + cls.trim().split(/\\s+/).slice(0, 3).join('.'); }
    return out;
}
var hit = null;
try { hit = document.elementFromPoint(cx, cy); } catch (err) { hit = null; }
var hitsSelf = false;
if (hit) { hitsSelf = (hit === el) || el.contains(hit) || hit.contains(el); }
return {
    'width': rect.width,
    'height': rect.height,
    'x': cx,
    'y': cy,
    'opacity': parseFloat(style.opacity),
    'visibility': style.visibility,
    'display': style.display,
    'pointerEvents': style.pointerEvents,
    'offscreen': (cx < 0 || cy < 0 ||
                  cx > window.innerWidth || cy > window.innerHeight),
    'hitsSelf': hitsSelf,
    'blocker': hitsSelf ? null : describe(hit),
    'target': describe(el),
    'text': (el.textContent || '').replace(/\\s+/g, ' ').trim().slice(0, 60)
};
"""


def _as_float(value, default):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return default if result != result else result  # NaN -> default


def probe_click_target(driver, element):
    """
    Measure one candidate in the live page, or None if it cannot be measured.

    None means "no information" (a driver with no execute_script, or a page
    that refused the script) and is deliberately distinct from a probe that
    came back saying the element is unclickable -- callers must not read the
    absence of a measurement as a bad measurement.
    """
    script = getattr(driver, 'execute_script', None)
    if script is None:
        return None
    try:
        info = script(_TARGET_PROBE_JS, element)
    except Exception:
        return None
    return info if isinstance(info, dict) else None


def classify_target(info):
    """
    Why a click on this candidate would or would not do anything.

      'phantom' -- matched the locator but cannot receive a click at all:
                   zero-sized, transparent, hidden, pointer-events:none, or
                   parked off-screen. This is the decoy case, and it is the
                   one is_displayed() gets wrong: opacity:0 is "displayed".
      'covered' -- real and on screen, but another element is on top at the
                   point a coordinate click would land, so the click would be
                   delivered to that element instead.
      'ok'      -- a coordinate click would land on it.

    Pure, so the classification can be tested against every shape of decoy
    without a browser. An unmeasurable candidate (info None) classifies as
    'ok': absent evidence, the click is still worth attempting.
    """
    if not info:
        return 'ok'
    if (info.get('display') == 'none'
            or info.get('visibility') in ('hidden', 'collapse')
            or info.get('pointerEvents') == 'none'
            or info.get('offscreen')
            or _as_float(info.get('opacity'), 1.0) <= 0.01
            or _as_float(info.get('width'), 1.0) <= 0.0
            or _as_float(info.get('height'), 1.0) <= 0.0):
        return 'phantom'
    if info.get('hitsSelf') is False:
        return 'covered'
    return 'ok'


def _is_displayed(element):
    """Fallback visibility for when the JS probe is unavailable."""
    check = getattr(element, 'is_displayed', None)
    if check is None:
        return True  # a parsed/fake element has nothing to hide behind
    try:
        return bool(check())
    except Exception:
        return False


def visible_cookie_buttons(driver, specs=None, limit=3):
    """
    Every consent-accept candidate on the page that could really take a click.

    Returns a list of (locator_description, element, info), in locator
    priority order, de-duplicated -- one button matches several of the phrase
    locators (the text "accept all cookies" contains "accept all", which
    contains "accept"), and clicking the same element four times is not four
    attempts at anything.

    Phantoms are dropped here, which is the whole point: they are what a
    plain find_element() would have handed back first.
    """
    if specs is None:
        specs = cookie_locator_specs(
            override=os.environ.get('ODDS_COOKIE_ACCEPT_XPATH'))

    found = []
    for kind, value in specs:
        by = BY_XPATH if kind == 'xpath' else BY_ID
        try:
            elements = driver.find_elements(by, value)
        except Exception:
            continue  # e.g. an id locator handed to an xpath-only fake driver
        for element in elements:
            if any(element is seen or element == seen for _, seen, _ in found):
                continue
            info = probe_click_target(driver, element)
            if info is None:
                if not _is_displayed(element):
                    continue
            elif classify_target(info) == 'phantom':
                continue
            found.append((f'{kind}={value}', element, info))
            if len(found) >= limit:
                return found
    return found


def cookie_banner_present(driver):
    """
    Is a consent banner currently showing and in the way?

    Was: any element matching a consent phrase EXISTS in the DOM. That could
    never answer the question, because every CMP dismisses its banner by
    hiding it and leaving the markup in place -- so this returned True just as
    loudly after a successful dismissal as before one. It is now the
    verification oracle accept_cookies() checks its own work against, so it
    has to mean "visible and clickable", not "present in the markup".
    """
    return bool(visible_cookie_buttons(driver, limit=1))


def _click(driver, element, scripted=False):
    """
    Click one element. Returns (delivered, note).

    `delivered` only ever means WebDriver accepted the click -- never that the
    page reacted to it. That distinction is the entire bug this file is about,
    so the caller checks the page separately and this function does not
    pretend to know.

    The scripted click dispatches on the element directly instead of at
    coordinates, so it is unaffected by anything painted on top of it. That
    also makes it the weaker signal: it will happily fire on an element with
    no handler, which is why it is a fallback and not the default.
    """
    if scripted:
        script = getattr(driver, 'execute_script', None)
        if script is None:
            return False, 'no-execute_script'
        try:
            script('arguments[0].click();', element)
            return True, 'ok'
        except Exception as exc:
            return False, type(exc).__name__
    try:
        element.click()
        return True, 'ok'
    except Exception as exc:
        return False, type(exc).__name__


def describe_cookie_failure(observations):
    """
    The message for a banner that survived every click delivered to it.

    Pure over already-collected observations, so the exact text a live failure
    produces is testable here rather than discovered on someone's console.
    Whoever hits this pastes it back, so it names the element that was clicked
    and what was on top of it -- the two facts that were missing every previous
    round of this bug.
    """
    lines = ['  ⚠ could not dismiss the cookie banner -- a matching accept '
             'button was found and clicked, and the banner is STILL showing '
             'afterwards. WebDriver accepted the click; the page ignored it.']
    for note in observations:
        label = note.get('target') or note.get('locator')
        if note.get('text'):
            label = f'{label} ("{note["text"]}")'
        outcome = ', '.join(f'{how}:{result}'
                            for how, result in note['clicks']) or 'not clicked'
        line = f'      {label} [{note["state"]}] -> {outcome}'
        if note['state'] == 'covered' and note.get('blocker'):
            line += f'; covered at its centre by {note["blocker"]}'
        lines.append(line)

    if observations and all(n['state'] == 'covered' for n in observations):
        lines.append('    Every match was covered by another element, so no '
                     'coordinate click could reach it. The scripted click '
                     'bypassed that and still changed nothing, which points '
                     'at the covering element above, not at the button.')
    else:
        lines.append('    A click WebDriver delivered that changes nothing '
                     'means the element it landed on carries no consent '
                     'handler: either a duplicate copy of the banner, or the '
                     'real button before its JS hydrated. Inspect the element '
                     'named above under ODDS_HEADLESS=0 and set '
                     'ODDS_COOKIE_ACCEPT_XPATH to target the real one.')
    return '\n'.join(lines)


def accept_cookies(driver, timeout=10, verify=2.0, attempts=3,
                   sleep=None, now=None):
    """
    Dismiss the cookie consent banner if it is showing. Never raises.

    Success is defined by the PAGE, not by the click call: every click is
    followed by a re-check that a clickable banner is no longer there, and
    only that re-check returns True. A click that WebDriver accepted and the
    page ignored is now a failure with a diagnosis attached, where before it
    was a silent success -- see the section comment above.

    The escalation per candidate, in order, stopping the moment the banner
    actually goes:
      1. a native coordinate click -- unless the probe already showed the
         candidate is covered, in which case a coordinate click provably
         cannot reach it and step 2 is used from the start;
      2. a scripted click on the element itself, which no overlay can block;
      3. the same again after a pause, for a handler that had not attached
         yet when the first click landed.
    Then on to the next candidate, because the first match in document order
    is not necessarily the live one.

    `timeout` bounds the wait for a clickable candidate to appear; every
    locator is re-checked in priority order on each poll, so a locator that
    never matches costs a poll rather than a share of the budget. `verify`
    bounds each wait for the banner to actually disappear (CMPs animate out).
    `sleep`/`now` are injectable so the retry behaviour is testable without
    real time passing.
    """
    import time as _time
    sleep = sleep or _time.sleep
    now = now or _time.monotonic

    deadline = now() + timeout
    while True:
        candidates = visible_cookie_buttons(driver)
        if candidates or now() >= deadline:
            break
        sleep(0.25)

    if not candidates:
        # Nothing clickable matched. Either there is no banner (the common,
        # harmless case) or there is one our patterns do not describe.
        print('  ⚠ could not dismiss the cookie banner -- none of the known '
              'accept-button patterns matched a visible element. If a banner '
              'is still showing under ODDS_HEADLESS=0, inspect the real '
              'button and set ODDS_COOKIE_ACCEPT_XPATH to an XPath for it.')
        return False

    observations = []
    for locator, element, info in candidates:
        state = classify_target(info)
        note = {'locator': locator, 'state': state, 'clicks': [],
                'target': (info or {}).get('target'),
                'text': (info or {}).get('text'),
                'blocker': (info or {}).get('blocker')}
        observations.append(note)

        for attempt in range(attempts):
            scripted = (state == 'covered') or attempt > 0
            delivered, result = _click(driver, element, scripted=scripted)
            note['clicks'].append(('scripted' if scripted else 'native', result))

            if delivered:
                gone_by = now() + verify
                while True:
                    if not cookie_banner_present(driver):
                        return True
                    if now() >= gone_by:
                        break
                    sleep(0.2)
            sleep(0.5)

    print(describe_cookie_failure(observations))
    return False
