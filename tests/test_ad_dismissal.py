"""
Reproduction and regression tests for the ad "sheet takeover" overlay that
blocks the Stats tab.

THE SYMPTOM
-----------
open_stats_tab() reported: "tab located but the click was intercepted --
it is covered by another element; the element on top at its centre is
div.sheet__backdrop". That backdrop belongs to an ad overlay, not the cookie
banner accept_cookies() already handles -- confirmed against a live capture
of the close button:

    <button aria-label="Close Advert"
            class="button button__icon-only button__icon-right
                   button--small button--text sheet__close-btn
                   js-sheet-takeover-close-button"></button>

It is icon-only (no visible text), so it has to be found by @aria-label or
by its classes, not by phrase-matched text content the way the cookie accept
button is.

This reuses the FakePage DOM simulator from test_cookie_dismissal.py rather
than redefining it -- the failure shape (decoy, covered, unhydrated handler)
is the same shape for any overlay, and dismiss_ad_overlay() shares its
click/verify mechanics with accept_cookies() via _dismiss_via_escalation().
What is actually new here is the LOCATOR: does it find the real button, by
the real attributes, and only that button.
"""

import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.browser import (
    ad_close_xpaths, ad_overlay_present, describe_ad_dismiss_failure,
    dismiss_ad_overlay, visible_ad_close_buttons,
)

from tests.test_cookie_dismissal import FakeClock, FakePage

# The real markup, as captured live, wrapped in the "sheet" container it
# actually sits in. data-banner marks it as belonging to the overlay for the
# fake's dismissal bookkeeping (see FakePage._dismiss); it is not part of the
# real page's markup.
REAL_AD_CLOSE_BUTTON = (
    '<button id="close-ad" data-banner="1" data-handler="works" '
    'aria-label="Close Advert" '
    'class="button button__icon-only button__icon-right button--small '
    'button--text sheet__close-btn js-sheet-takeover-close-button">'
    '</button>')

DEAD_AD_CLOSE_BUTTON = (
    '<button id="dead-ad" data-banner="1" data-handler="dead" '
    'aria-label="Close Advert" class="sheet__close-btn">'
    '</button>')


def run(page, **kwargs):
    """Call dismiss_ad_overlay against a fake page, capturing what it printed."""
    clock = FakeClock()
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        result = dismiss_ad_overlay(page, sleep=clock.sleep, now=clock.now,
                                    **kwargs)
    return result, buffer.getvalue()


class TestAdCloseLocators(unittest.TestCase):
    """The real button, found by the real attributes -- not by position."""

    def test_finds_the_real_button_by_aria_label(self):
        page = FakePage(f'<div>{REAL_AD_CLOSE_BUTTON}</div>')
        found = visible_ad_close_buttons(page)
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0][1]._node.get('id'), 'close-ad')

    def test_case_and_wording_variants_are_not_required_to_match_exactly(self):
        # A vendor that ships "Close Ad" or "CLOSE ADVERT" instead should
        # still match -- the phrase condition lower-cases both sides.
        page = FakePage(
            '<div><button id="variant" data-banner="1" data-handler="works" '
            'aria-label="CLOSE AD"></button></div>')
        found = visible_ad_close_buttons(page)
        self.assertEqual(len(found), 1)

    def test_falls_back_to_class_when_aria_label_is_unrecognised(self):
        # If the vendor changes the aria-label wording entirely, the two
        # BEM classes from the live capture are still a second, independent
        # way to find the same button.
        page = FakePage(
            '<div><button id="no-label" data-banner="1" data-handler="works" '
            'class="sheet__close-btn"></button></div>')
        found = visible_ad_close_buttons(page)
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0][1]._node.get('id'), 'no-label')

    def test_does_not_accidentally_match_the_cookie_accept_button(self):
        page = FakePage(
            '<div><button id="cookie" data-banner="1" data-handler="works">'
            'Accept All Cookies</button></div>')
        self.assertEqual(visible_ad_close_buttons(page), [])

    def test_xpaths_are_well_formed_xpath(self):
        # Pure sanity check on the generated expressions themselves, no
        # driver needed -- catches a syntax mistake in the predicate before
        # it ever reaches a real page.
        from lxml import etree
        doc = etree.fromstring(f'<div>{REAL_AD_CLOSE_BUTTON}</div>')
        for xp in ad_close_xpaths():
            doc.xpath(xp)  # raises XPathSyntaxError on anything malformed


class TestAdOverlayPresent(unittest.TestCase):
    def test_true_while_the_close_button_is_visible(self):
        page = FakePage(f'<div>{REAL_AD_CLOSE_BUTTON}</div>')
        self.assertTrue(ad_overlay_present(page))

    def test_false_once_dismissed_not_merely_hidden_from_a_stale_check(self):
        page = FakePage(f'<div>{REAL_AD_CLOSE_BUTTON}</div>')
        page.find_elements('xpath', '//button')[0].click()
        self.assertFalse(ad_overlay_present(page))

    def test_false_with_no_overlay_on_the_page(self):
        page = FakePage('<div><p>no ad here</p></div>')
        self.assertFalse(ad_overlay_present(page))


class TestDismissAdOverlay(unittest.TestCase):
    def test_the_plain_case_closes_the_overlay(self):
        page = FakePage(f'<div>{REAL_AD_CLOSE_BUTTON}</div>')
        result, output = run(page)
        self.assertTrue(result)
        self.assertTrue(page.dismissed)
        self.assertEqual(output, '')

    def test_a_covered_close_button_is_clicked_by_script_instead(self):
        page = FakePage(
            '<div><button id="close-ad" data-banner="1" data-handler="works" '
            'aria-label="Close Advert" data-blocker="div.sheet__backdrop">'
            '</button></div>')
        result, output = run(page)
        self.assertTrue(result)
        self.assertIn('close-ad', [e._node.get('id') for e in page.scripted])

    def test_no_overlay_present_is_reported_not_raised(self):
        page = FakePage('<div><p>nothing to see</p></div>')
        result, output = run(page, timeout=0)
        self.assertFalse(result)
        self.assertIn('none of the known close-button patterns', output)
        self.assertIn('ODDS_AD_CLOSE_XPATH', output)
        # Must not reuse the cookie-banner wording -- it would misdirect
        # whoever reads it toward accept_cookies() instead of here.
        self.assertNotIn('cookie', output)

    def test_a_dead_button_is_reported_with_the_element_named(self):
        page = FakePage(f'<div>{DEAD_AD_CLOSE_BUTTON}</div>')
        result, output = run(page)
        self.assertFalse(result)
        self.assertIn('dead-ad', output)
        self.assertIn('STILL showing', output)
        self.assertIn('no close handler', output)

    def test_never_raises_on_a_hostile_driver(self):
        class Broken:
            def find_elements(self, by, value):
                raise RuntimeError('no such session')
        clock = FakeClock()
        with redirect_stdout(io.StringIO()):
            self.assertFalse(dismiss_ad_overlay(
                Broken(), timeout=0, sleep=clock.sleep, now=clock.now))


class TestDescribeAdDismissFailure(unittest.TestCase):
    def test_names_the_button_and_does_not_say_consent(self):
        message = describe_ad_dismiss_failure([{
            'locator': 'xpath=//button', 'state': 'ok', 'blocker': None,
            'target': 'button#close-ad', 'text': '',
            'clicks': [('native', 'ok'), ('scripted', 'ok')],
        }])
        self.assertIn('button#close-ad', message)
        self.assertIn('no close handler', message)
        self.assertNotIn('consent', message)

    def test_covered_gets_its_own_explanation(self):
        message = describe_ad_dismiss_failure([{
            'locator': 'xpath=//button', 'state': 'covered',
            'blocker': 'div.sheet__backdrop', 'target': 'button#close-ad',
            'text': '', 'clicks': [('scripted', 'ok')],
        }])
        self.assertIn('div.sheet__backdrop', message)
        self.assertIn('covered by another element', message)


if __name__ == '__main__':
    unittest.main()
