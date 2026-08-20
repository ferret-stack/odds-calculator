"""
Tests for the cookie-consent locator logic.

Covers the reported live failure: a banner whose actual accept button did
not match any of the three original locators (a OneTrust id and two
button-scoped "Accept..." text matches), leaving the banner standing and
blocking whatever it covered underneath (the Stats tab, in the reports seen
so far).
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.browser import _COOKIE_PHRASES, _phrase_condition, cookie_accept_xpaths


class TestCookiePhrases(unittest.TestCase):
    def test_common_phrases_covered(self):
        for phrase in ('accept', 'accept all', 'agree', 'allow all', 'got it'):
            self.assertIn(phrase, _COOKIE_PHRASES)

    def test_specific_phrases_precede_generic_ones(self):
        """
        The first clickable match wins, so a precise phrase like
        "accept all cookies" must be tried before the bare "accept" it
        contains, or the more precise button could be skipped in favour of
        an unrelated element that merely contains the word "accept".
        """
        specific = _COOKIE_PHRASES.index('accept all cookies')
        generic = _COOKIE_PHRASES.index('accept')
        self.assertLess(specific, generic)

        specific2 = _COOKIE_PHRASES.index('allow all cookies')
        generic2 = _COOKIE_PHRASES.index('allow all')
        self.assertLess(specific2, generic2)


class TestPhraseCondition(unittest.TestCase):
    def test_condition_is_case_insensitive_via_translate(self):
        cond = _phrase_condition('accept')
        self.assertIn('translate(', cond)
        self.assertIn("'accept'", cond)

    def test_well_formed_xpath_predicate(self):
        """Sanity: the condition parses as a real XPath predicate."""
        from lxml import etree
        cond = _phrase_condition('accept all')
        doc = etree.fromstring('<button>Accept All</button>')
        # Should not raise -- proves the generated XPath is syntactically valid.
        result = doc.xpath(f'boolean({cond})')
        self.assertTrue(result)

    def test_matches_regardless_of_case(self):
        from lxml import etree
        cond = _phrase_condition('accept all')
        for text in ('Accept All', 'ACCEPT ALL', 'accept all', 'AcCePt AlL'):
            doc = etree.fromstring(f'<button>{text}</button>')
            self.assertTrue(doc.xpath(f'boolean({cond})'), text)


class TestCookieAcceptXpaths(unittest.TestCase):
    def test_one_xpath_per_phrase(self):
        self.assertEqual(len(cookie_accept_xpaths()), len(_COOKIE_PHRASES))

    def test_not_scoped_to_button_tag_only(self):
        """
        The original failure: a bespoke banner used a non-<button> element.
        Every generated xpath must also reach <a> and any [role=button].
        """
        for xp in cookie_accept_xpaths():
            self.assertIn('//a[', xp)
            self.assertIn("@role='button'", xp)

    def test_real_dom_match_for_a_non_button_element(self):
        """
        Reproduces the exact live scenario: a div[role=button] reading
        "I Agree" -- not a <button>, not the literal word "accept".
        """
        from lxml import etree
        doc = etree.fromstring(
            '<div><div role="button" class="consent-accept">I Agree</div></div>')
        xpaths = cookie_accept_xpaths()
        matched = any(doc.xpath(xp) for xp in xpaths)
        self.assertTrue(matched, 'no phrase-derived xpath matched the div[role=button]')

    def test_plain_button_with_original_wording_still_matches(self):
        """The original working case must still work."""
        from lxml import etree
        doc = etree.fromstring('<button>Accept All Cookies</button>')
        xpaths = cookie_accept_xpaths()
        self.assertTrue(any(doc.xpath(xp) for xp in xpaths))

    def test_unrelated_button_not_matched(self):
        from lxml import etree
        doc = etree.fromstring('<button>Read more</button>')
        xpaths = cookie_accept_xpaths()
        self.assertFalse(any(doc.xpath(xp) for xp in xpaths))


if __name__ == '__main__':
    unittest.main(verbosity=2)
