"""
Reproduction and regression tests for the cookie banner that would not go away.

THE SYMPTOM THESE REPRODUCE
---------------------------
accept_cookies() reported success -- a locator matched, .click() returned
without raising, no warning printed -- and the banner stayed visible for the
whole run, still covering the Stats tab, which then failed with
ElementClickInterceptedException.

Two earlier rounds fixed locator COVERAGE (the accept button did not match any
pattern) and locator PRIORITY (a never-matching OneTrust id consumed the whole
timeout budget). Both were real, both are fixed, and neither is what this file
is about. This is the round after: matching is correct, the click is delivered,
and the page does not change.

premierleague.com is unreachable from where this fix was written, so the page
is modelled instead. FakePage below is a small DOM simulator with the three
properties that a plain "did .click() raise?" check cannot see:

  * an element can be VISIBLE TO SELENIUM BUT NOT TO A USER. is_displayed()
    returns True for an opacity:0 element -- that is a documented Selenium
    behaviour, not a quirk of this fake -- so a transparent duplicate of the
    banner is a perfectly good click target as far as the old code could tell.
  * an element can be COVERED. A coordinate click on it is delivered to
    whatever is painted on top instead.
  * an element can HAVE NO HANDLER YET. Server-rendered markup takes the click
    silently until its JS hydrates and attaches one.

test_the_fake_reproduces_the_reported_symptom asserts the fake really does
reproduce it, before anything else asserts the fix works against it.
"""

import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers import browser
from scrapers.browser import (
    _TARGET_PROBE_JS, accept_cookies, classify_target, cookie_banner_present,
    describe_cookie_failure, visible_cookie_buttons,
)


class ElementClickInterceptedException(Exception):
    """Named to match the real Selenium exception the fake stands in for."""


class FakeElement:
    """A handle onto one node of the fake page, in the shape Selenium uses."""

    def __init__(self, page, node):
        self._page = page
        self._node = node

    # Real WebElements compare by the remote element id, not by handle
    # identity -- find_elements() twice gives two handles to one element.
    # visible_cookie_buttons() de-duplicates on that, so the fake must too.
    def __eq__(self, other):
        return isinstance(other, FakeElement) and other._node is self._node

    def __hash__(self):
        return id(self._node)

    def _attr(self, name, default):
        return self._node.get(name, default)

    def is_displayed(self):
        """
        Deliberately faithful to Selenium, including the part that misleads:
        display:none and visibility:hidden are not displayed, opacity:0 IS.
        """
        return (self._attr('data-display', 'block') != 'none'
                and self._attr('data-visibility', 'visible') != 'hidden')

    def click(self):
        if self._page.dismissed:
            return
        if self._attr('data-blocker', ''):
            raise ElementClickInterceptedException(
                'element click intercepted: another element would receive it')
        self._page.deliver(self)


class FakePage:
    """
    A driver-shaped DOM simulator.

    Visual state rides on data-* attributes so the markup in each test reads
    as the page it stands for:
        data-opacity/-display/-visibility/-w/-h  what the probe measures
        data-blocker                             what is painted on top
        data-handler                             works | dead | hydrate:N
    """

    def __init__(self, html):
        from lxml import etree
        self._doc = etree.fromstring(html)
        self.dismissed = False
        self.delivered = []          # every click the page actually received
        self.scripted = []           # which of those were scripted

    # -- driver surface ----------------------------------------------------
    def find_elements(self, by, value):
        xpath = value if by == 'xpath' else f"//*[@id='{value}']"
        nodes = self._doc.xpath(xpath)
        return [FakeElement(self, n) for n in nodes
                if not (self.dismissed and n.get('data-banner'))]

    def execute_script(self, script, *args):
        element = args[0]
        if 'elementFromPoint' in script:
            return self._probe(element)
        if 'click' in script:
            self.scripted.append(element)
            self.deliver(element)
            return None
        raise AssertionError(f'unexpected script: {script[:40]}')

    # -- page behaviour ----------------------------------------------------
    def _probe(self, element):
        node = element._node
        get = node.get
        return {
            'width': float(get('data-w', '200')),
            'height': float(get('data-h', '48')),
            'x': 100.0, 'y': 100.0,
            'opacity': float(get('data-opacity', '1')),
            'visibility': get('data-visibility', 'visible'),
            'display': get('data-display', 'block'),
            'pointerEvents': get('data-pointer-events', 'auto'),
            'offscreen': get('data-offscreen', '') == '1',
            'hitsSelf': not get('data-blocker', ''),
            'blocker': get('data-blocker', '') or None,
            'target': node.tag + ('#' + get('id') if get('id') else ''),
            'text': (node.text or '').strip(),
        }

    def deliver(self, element):
        """A click reached this element. Whether anything happens is the page's call."""
        self.delivered.append(element)
        handler = element._node.get('data-handler', 'dead')
        if handler == 'works':
            self._dismiss()
        elif handler.startswith('hydrate:'):
            # The handler attaches only after N clicks have gone nowhere.
            needed = int(handler.split(':')[1])
            if len(self.delivered) >= needed:
                self._dismiss()

    def _dismiss(self):
        self.dismissed = True
        for node in self._doc.xpath('//*[@data-banner]'):
            node.set('data-display', 'none')


# Clock and sleep that never actually wait; time only moves when slept on.
class FakeClock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.t += seconds


def run(page, **kwargs):
    """Call accept_cookies against a fake page, capturing what it printed."""
    clock = FakeClock()
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        result = accept_cookies(page, sleep=clock.sleep, now=clock.now, **kwargs)
    return result, buffer.getvalue()


REAL_BUTTON = ('<button id="real" data-banner="1" data-handler="works">'
               'Accept All Cookies</button>')
DEAD_BUTTON = ('<button id="dead" data-banner="1" data-handler="dead">'
               'Accept All Cookies</button>')


class TestTheFakeItself(unittest.TestCase):
    """
    The fake has to reproduce the reported failure before it is worth using
    to test a fix for it. Same discipline as the ELO fix: re-run the thing
    that was broken and watch it break.
    """

    def test_the_fake_reproduces_the_reported_symptom(self):
        """
        The old success criterion, applied to a transparent duplicate: the
        element is found, is_displayed() says True, .click() does not raise,
        and the banner is still there. That is the console output verbatim --
        a success with no effect and no warning.
        """
        page = FakePage(
            '<div>'
            '<button id="ghost" data-banner="1" data-opacity="0" '
            'data-handler="dead">Accept All Cookies</button>'
            + REAL_BUTTON +
            '</div>')

        first = page.find_elements('xpath', '//button')[0]
        self.assertEqual(first._node.get('id'), 'ghost',
                         'the decoy must come first in document order')
        self.assertTrue(first.is_displayed(),
                        'Selenium reports an opacity:0 element as displayed')
        first.click()  # must NOT raise -- this is the whole problem
        self.assertFalse(page.dismissed,
                         'the decoy click must leave the banner standing')

    def test_a_working_button_dismisses_and_hides_the_banner(self):
        page = FakePage(f'<div>{REAL_BUTTON}</div>')
        page.find_elements('xpath', '//button')[0].click()
        self.assertTrue(page.dismissed)
        self.assertFalse(cookie_banner_present(page))


class TestClassifyTarget(unittest.TestCase):
    """The pure classifier, over every shape of decoy seen in the wild."""

    BASE = {'width': 200.0, 'height': 48.0, 'opacity': 1.0,
            'visibility': 'visible', 'display': 'block',
            'pointerEvents': 'auto', 'offscreen': False, 'hitsSelf': True}

    def info(self, **overrides):
        merged = dict(self.BASE)
        merged.update(overrides)
        return merged

    def test_a_normal_button_is_ok(self):
        self.assertEqual(classify_target(self.info()), 'ok')

    def test_transparent_is_phantom(self):
        """The case is_displayed() gets wrong, and the likeliest culprit here."""
        self.assertEqual(classify_target(self.info(opacity=0.0)), 'phantom')

    def test_zero_sized_is_phantom(self):
        self.assertEqual(classify_target(self.info(width=0.0)), 'phantom')
        self.assertEqual(classify_target(self.info(height=0.0)), 'phantom')

    def test_hidden_is_phantom(self):
        self.assertEqual(classify_target(self.info(display='none')), 'phantom')
        self.assertEqual(classify_target(self.info(visibility='hidden')), 'phantom')

    def test_unclickable_or_parked_offscreen_is_phantom(self):
        self.assertEqual(classify_target(self.info(pointerEvents='none')), 'phantom')
        self.assertEqual(classify_target(self.info(offscreen=True)), 'phantom')

    def test_something_on_top_is_covered(self):
        self.assertEqual(
            classify_target(self.info(hitsSelf=False, blocker='div#overlay')),
            'covered')

    def test_no_measurement_is_not_treated_as_a_bad_measurement(self):
        """Absent evidence the click is still worth attempting."""
        self.assertEqual(classify_target(None), 'ok')


class TestVisibleCookieButtons(unittest.TestCase):
    def test_phantoms_are_excluded(self):
        page = FakePage(
            '<div>'
            '<button data-banner="1" data-opacity="0">Accept All Cookies</button>'
            + REAL_BUTTON +
            '</div>')
        found = visible_cookie_buttons(page)
        self.assertEqual([e._node.get('id') for _, e, _ in found], ['real'])

    def test_one_button_is_not_four_candidates(self):
        """
        "accept all cookies" also matches the "accept all" and "accept"
        locators. Clicking the same element once per phrase is not four
        attempts at anything, and would quadruple the failure output.
        """
        page = FakePage(f'<div>{REAL_BUTTON}</div>')
        self.assertEqual(len(visible_cookie_buttons(page)), 1)

    def test_present_means_visible_not_merely_in_the_dom(self):
        """
        The regression that made the old check useless as an oracle: every
        CMP dismisses by hiding, leaving the markup in place, so an
        existence check answers True just as loudly after a successful
        dismissal as before one.
        """
        page = FakePage('<div><button data-display="none">'
                        'Accept All Cookies</button></div>')
        self.assertFalse(cookie_banner_present(page))


class TestAcceptCookies(unittest.TestCase):
    def test_plain_case_still_works(self):
        page = FakePage(f'<div>{REAL_BUTTON}</div>')
        result, output = run(page)
        self.assertTrue(result)
        self.assertTrue(page.dismissed)
        self.assertEqual(output, '', 'a clean dismissal must stay quiet')

    def test_decoy_first_no_longer_swallows_the_dismissal(self):
        """The reported symptom, fixed: skip the transparent copy, click the real one."""
        page = FakePage(
            '<div>'
            '<button id="ghost" data-banner="1" data-opacity="0" '
            'data-handler="dead">Accept All Cookies</button>'
            + REAL_BUTTON +
            '</div>')
        result, output = run(page)
        self.assertTrue(result)
        self.assertTrue(page.dismissed)
        self.assertEqual(output, '')
        self.assertNotIn('ghost', [e._node.get('id') for e in page.delivered],
                         'the decoy must never have been clicked')

    def test_unhydrated_button_is_retried_until_its_handler_attaches(self):
        """
        Hypothesis (a): the markup is there, the listener is not yet. The
        first click is delivered and does nothing; a later one works.
        """
        page = FakePage('<div><button id="slow" data-banner="1" '
                        'data-handler="hydrate:2">Accept All Cookies</button></div>')
        result, output = run(page)
        self.assertTrue(result)
        self.assertTrue(page.dismissed)
        self.assertEqual(len(page.delivered), 2,
                         'should have taken exactly two clicks')
        self.assertEqual(output, '')

    def test_covered_button_is_clicked_by_script_instead_of_coordinates(self):
        """
        Hypothesis (c): a coordinate click provably cannot reach a covered
        element, so it is not wasted -- the scripted click is used from the
        start, and no overlay can block it.
        """
        page = FakePage('<div><button id="under" data-banner="1" '
                        'data-blocker="div#promo-overlay" data-handler="works">'
                        'Accept All Cookies</button></div>')
        result, output = run(page)
        self.assertTrue(result)
        self.assertTrue(page.dismissed)
        self.assertEqual(len(page.scripted), 1)
        self.assertEqual(len(page.delivered), 1, 'no wasted coordinate click')

    def test_a_click_that_changes_nothing_is_a_failure_now_not_a_success(self):
        """
        THE REGRESSION THAT MATTERS. Before: .click() did not raise, so
        accept_cookies() returned True and printed nothing, and the real
        cause surfaced later as an unexplained ElementClickInterceptedException
        on the Stats tab. Now the page is asked, and it says no.
        """
        page = FakePage(f'<div>{DEAD_BUTTON}</div>')
        result, output = run(page)
        self.assertFalse(result)
        self.assertIn('STILL showing', output)
        self.assertIn('button#dead', output, 'must name what it clicked')
        self.assertNotIn('none of the known', output,
                         'this is not a matching failure and must not claim to be')

    def test_failure_output_names_the_covering_element(self):
        page = FakePage('<div><button id="under" data-banner="1" '
                        'data-blocker="div#promo-overlay" data-handler="dead">'
                        'Accept All Cookies</button></div>')
        result, output = run(page)
        self.assertFalse(result)
        self.assertIn('div#promo-overlay', output)

    def test_nothing_matching_is_reported_as_a_matching_failure(self):
        page = FakePage('<div><button>Read more</button></div>')
        result, output = run(page, timeout=0)
        self.assertFalse(result)
        self.assertIn('none of the known', output)
        self.assertIn('ODDS_COOKIE_ACCEPT_XPATH', output)

    def test_a_banner_that_appears_late_is_still_caught(self):
        """timeout is a budget for the banner to show up, not a fixed wait."""
        page = FakePage(f'<div>{REAL_BUTTON}</div>')
        page._doc.xpath('//button')[0].set('data-display', 'none')

        clock = FakeClock()
        original = page.find_elements

        def appear_after_a_second(by, value):
            if clock.t >= 1.0:
                page._doc.xpath('//button')[0].set('data-display', 'block')
            return original(by, value)

        page.find_elements = appear_after_a_second
        with redirect_stdout(io.StringIO()):
            result = accept_cookies(page, sleep=clock.sleep, now=clock.now)
        self.assertTrue(result)
        self.assertTrue(page.dismissed)

    def test_never_raises_on_a_hostile_driver(self):
        """A dead driver must not take the whole scrape down with it."""
        class Broken:
            def find_elements(self, by, value):
                raise RuntimeError('session deleted')

        clock = FakeClock()
        with redirect_stdout(io.StringIO()):
            self.assertFalse(accept_cookies(Broken(), timeout=0,
                                            sleep=clock.sleep, now=clock.now))


class TestDescribeCookieFailure(unittest.TestCase):
    """
    The failure text is the diagnostic interface -- whoever hits this pastes
    it back -- so its content is asserted, not left to chance.
    """

    def test_reports_the_element_and_how_it_was_clicked(self):
        message = describe_cookie_failure([{
            'locator': 'xpath=//button', 'state': 'ok', 'blocker': None,
            'target': 'button#accept', 'text': 'Accept All Cookies',
            'clicks': [('native', 'ok'), ('scripted', 'ok')],
        }])
        self.assertIn('button#accept', message)
        self.assertIn('Accept All Cookies', message)
        self.assertIn('native:ok', message)
        self.assertIn('scripted:ok', message)
        self.assertIn('no consent handler', message)

    def test_all_covered_gets_its_own_explanation(self):
        message = describe_cookie_failure([{
            'locator': 'xpath=//button', 'state': 'covered',
            'blocker': 'div#overlay', 'target': 'button#accept', 'text': 'Accept',
            'clicks': [('scripted', 'ok')],
        }])
        self.assertIn('div#overlay', message)
        self.assertIn('covered by another element', message)
        self.assertNotIn('no consent handler', message)

    def test_falls_back_to_the_locator_when_the_probe_said_nothing(self):
        message = describe_cookie_failure([{
            'locator': 'id=onetrust-accept-btn-handler', 'state': 'ok',
            'blocker': None, 'target': None, 'text': None,
            'clicks': [('native', 'ok')],
        }])
        self.assertIn('id=onetrust-accept-btn-handler', message)


class TestProbeScript(unittest.TestCase):
    def test_probe_returns_none_when_the_driver_cannot_run_scripts(self):
        class NoScript:
            pass
        self.assertIsNone(browser.probe_click_target(NoScript(), object()))

    def test_probe_returns_none_when_the_page_refuses_the_script(self):
        class Refuses:
            def execute_script(self, script, *args):
                raise RuntimeError('javascript disabled')
        self.assertIsNone(browser.probe_click_target(Refuses(), object()))

    def test_probe_script_asks_for_the_topmost_element(self):
        """Without elementFromPoint there is no way to see an overlay at all."""
        self.assertIn('elementFromPoint', _TARGET_PROBE_JS)
        self.assertIn('getBoundingClientRect', _TARGET_PROBE_JS)
        self.assertIn('getComputedStyle', _TARGET_PROBE_JS)


if __name__ == '__main__':
    unittest.main(verbosity=2)


def _selenium_available():
    try:
        import selenium  # noqa: F401
        return True
    except Exception:
        return False


@unittest.skipUnless(_selenium_available(), 'needs selenium for WebDriverWait')
class TestStatsTabInterceptDiagnostic(unittest.TestCase):
    """
    The other half of the reported console line. "covered by another element,
    e.g. an undismissed cookie banner" names a suspect, not the culprit -- and
    guessing at the culprit is exactly what cost this bug several rounds. The
    page already knows which element is on top; ask it.
    """

    class _Tab:
        def __init__(self, exc):
            self._exc = exc

        def is_displayed(self):
            return True

        def is_enabled(self):
            return True

        def click(self):
            raise self._exc

    class _Driver:
        def __init__(self, tab, blocker):
            self._tab = tab
            self._blocker = blocker

        def find_element(self, by, value):
            return self._tab

        def execute_script(self, script, *args):
            if 'elementFromPoint' in script:
                return {'hitsSelf': False, 'blocker': self._blocker,
                        'target': 'button#stats-tab', 'text': 'Stats'}
            raise AssertionError('unexpected script')

    def _run(self, blocker):
        from selenium.common.exceptions import ElementClickInterceptedException
        from scrapers.match_stats import open_stats_tab
        tab = self._Tab(ElementClickInterceptedException('intercepted'))
        return open_stats_tab(self._Driver(tab, blocker), timeout=1)

    def test_names_the_element_that_is_actually_on_top(self):
        opened, note = self._run('div#onetrust-banner-sdk')
        self.assertFalse(opened)
        self.assertIn('div#onetrust-banner-sdk', note)
        self.assertIn('intercepted', note)

    def test_falls_back_to_the_old_wording_when_the_probe_says_nothing(self):
        opened, note = self._run(None)
        self.assertFalse(opened)
        self.assertIn('undismissed cookie banner', note)
