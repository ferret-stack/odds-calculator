"""
Tests for the advanced-stats parser.

These cover the two defects the Day 2 fix addresses:
  * cards silently coming back as 0 when nothing was read
  * 0 being indistinguishable from "not scraped"
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.match_stats import (
    MIN_RECOGNISED_ROWS, booking_points, build_result, empty_result,
    normalise_label, orient_triple, parse_stat_rows, parse_stat_value,
    _best_table, _diagnose_no_match, _stats_content_ready,
    _wait_for_stats_content,
)


class TestParseStatValue(unittest.TestCase):
    def test_plain_integers(self):
        self.assertEqual(parse_stat_value('3', 'yellow'), 3)
        self.assertEqual(parse_stat_value(' 12 ', 'fouls'), 12)

    def test_zero_is_a_real_value(self):
        self.assertEqual(parse_stat_value('0', 'yellow'), 0)
        self.assertIsNotNone(parse_stat_value('0', 'red'))

    def test_blank_and_dash_are_unknown_not_zero(self):
        for blank in ('', '   ', '-', '--', 'N/A', None):
            self.assertIsNone(parse_stat_value(blank, 'yellow'), blank)

    def test_percentages(self):
        self.assertEqual(parse_stat_value('62%', 'possession'), 62)
        self.assertEqual(parse_stat_value('83.5%', 'pass_accuracy'), 83.5)

    def test_thousands_separator(self):
        self.assertEqual(parse_stat_value('1,004', 'passes'), 1004)

    def test_implausible_values_rejected(self):
        self.assertIsNone(parse_stat_value('99', 'yellow'))
        self.assertIsNone(parse_stat_value('150', 'possession'))

    def test_fractional_count_rejected(self):
        self.assertIsNone(parse_stat_value('2.5', 'yellow'))

    def test_xg_keeps_decimals(self):
        self.assertEqual(parse_stat_value('1.87', 'xg'), 1.87)


class TestParseStatRows(unittest.TestCase):
    ROWS = [
        ('Possession %', '62', '38'),
        ('Shots', '17', '9'),
        ('Shots on target', '6', '3'),
        ('Yellow cards', '2', '4'),
        ('Red cards', '0', '1'),
        ('Fouls', '9', '14'),
        ('Corners', '8', '2'),
    ]

    def test_extracts_known_rows(self):
        parsed = parse_stat_rows(self.ROWS)
        self.assertEqual(parsed['home']['yellow'], 2)
        self.assertEqual(parsed['away']['yellow'], 4)
        self.assertEqual(parsed['home']['red'], 0)
        self.assertEqual(parsed['away']['red'], 1)
        self.assertEqual(parsed['home']['possession'], 62)

    def test_unknown_labels_ignored(self):
        parsed = parse_stat_rows(self.ROWS + [('Big chances created', '4', '1')])
        self.assertNotIn('big_chances_created', parsed['home'])

    def test_row_order_does_not_matter(self):
        forward = parse_stat_rows(self.ROWS)
        backward = parse_stat_rows(list(reversed(self.ROWS)))
        self.assertEqual(forward, backward)

    def test_label_variants(self):
        parsed = parse_stat_rows([('Total shots', '11', '5'),
                                  ('Shots on goal', '4', '2')])
        self.assertEqual(parsed['home']['shots'], 11)
        self.assertEqual(parsed['home']['shots_on_target'], 4)

    def test_label_normalisation(self):
        self.assertEqual(normalise_label('  Yellow   Cards :'), 'yellow cards')


class TestBookingPoints(unittest.TestCase):
    def test_standard_scoring(self):
        self.assertEqual(booking_points(2, 0), 20)
        self.assertEqual(booking_points(3, 1), 55)

    def test_zero_cards_scores_zero(self):
        self.assertEqual(booking_points(0, 0), 0)

    def test_unknown_stays_unknown(self):
        """The regression that mattered: unknown must not become 0."""
        self.assertIsNone(booking_points(None, None))


class TestBuildResult(unittest.TestCase):
    def test_full_row_set(self):
        res = build_result(parse_stat_rows(TestParseStatRows.ROWS))
        self.assertTrue(res['stats_scraped'])
        self.assertEqual(res['home_yellow'], 2)
        self.assertEqual(res['away_red'], 1)
        self.assertEqual(res['home_booking_points'], 20)   # 2y
        self.assertEqual(res['away_booking_points'], 65)   # 4y + 1r
        self.assertEqual(res['total_booking_points'], 85)

    def test_missing_red_row_means_zero_reds(self):
        res = build_result(parse_stat_rows([('Yellow cards', '1', '2')]))
        self.assertEqual(res['home_red'], 0)
        self.assertEqual(res['total_booking_points'], 30)

    def test_no_known_rows_is_not_scraped(self):
        res = build_result(parse_stat_rows([('Big chances', '4', '1')]))
        self.assertFalse(res['stats_scraped'])
        self.assertIsNone(res['total_booking_points'])
        self.assertIsNone(res['home_yellow'])

    def test_empty_result_shape(self):
        res = empty_result('boom')
        self.assertFalse(res['stats_scraped'])
        self.assertEqual(res['stats_error'], 'boom')
        for field in ('home_yellow', 'away_yellow', 'home_red', 'away_red',
                      'total_booking_points'):
            self.assertIsNone(res[field], field)

    def test_genuine_zero_cards_distinguishable_from_unscraped(self):
        """A 0-0 card match and an unscraped match must not look alike."""
        scraped = build_result(parse_stat_rows(
            [('Yellow cards', '0', '0'), ('Red cards', '0', '0')]))
        unscraped = empty_result('tab not found')

        self.assertTrue(scraped['stats_scraped'])
        self.assertEqual(scraped['total_booking_points'], 0)

        self.assertFalse(unscraped['stats_scraped'])
        self.assertIsNone(unscraped['total_booking_points'])


class TestOrientTriple(unittest.TestCase):
    """
    Covers the real-world failure: rows found, none recognised, because the
    label wasn't in the column this module assumed.
    """

    def test_label_in_middle_column(self):
        # (home, label, away) -- the original assumption.
        self.assertEqual(orient_triple('2', 'Yellow cards', '4'),
                         ('Yellow cards', '2', '4'))

    def test_label_in_first_column(self):
        # (label, home, away) -- the layout that broke the original code.
        self.assertEqual(orient_triple('Yellow cards', '2', '4'),
                         ('Yellow cards', '2', '4'))

    def test_neither_column_recognised_returns_none(self):
        self.assertIsNone(orient_triple('Some Widget', 'Link', 'More'))

    def test_ambiguous_row_prefers_first_column(self):
        """
        If both columns happen to look like labels (unlikely, but the tie
        rule should be deterministic), the first position wins.
        """
        result = orient_triple('Fouls', 'Corners', '3')
        self.assertEqual(result[0], 'Fouls')


class TestBestTable(unittest.TestCase):
    STATS_TABLE_MIDDLE_LABEL = [
        ('62', 'Possession %', '38'),
        ('17', 'Shots', '9'),
        ('2', 'Yellow cards', '4'),
        ('0', 'Red cards', '1'),
    ]
    STATS_TABLE_FIRST_LABEL = [
        ('Possession %', '62', '38'),
        ('Shots', '17', '9'),
        ('Yellow cards', '2', '4'),
        ('Red cards', '0', '1'),
    ]
    UNRELATED_TABLE = [
        ('Related', 'Read more', 'Arsenal beat Chelsea'),
        ('Related', 'Read more', 'Everton sign new winger'),
        ('Related', 'Read more', 'Transfer news roundup'),
    ]

    def test_picks_up_middle_label_layout(self):
        result = _best_table([self.STATS_TABLE_MIDDLE_LABEL])
        self.assertIsNotNone(result)
        self.assertIn(('Yellow cards', '2', '4'), result)

    def test_picks_up_first_label_layout(self):
        """The layout that produced 'rows found, none matched' in the field."""
        result = _best_table([self.STATS_TABLE_FIRST_LABEL])
        self.assertIsNotNone(result)
        self.assertIn(('Yellow cards', '2', '4'), result)

    def test_unrelated_table_rejected(self):
        self.assertIsNone(_best_table([self.UNRELATED_TABLE]))

    def test_real_table_picked_over_unrelated_one_on_the_same_page(self):
        result = _best_table([self.UNRELATED_TABLE, self.STATS_TABLE_FIRST_LABEL])
        self.assertIsNotNone(result)
        self.assertIn(('Yellow cards', '2', '4'), result)

    def test_single_recognised_row_not_enough(self):
        """Below MIN_RECOGNISED_ROWS, a table is not trusted at all."""
        self.assertEqual(MIN_RECOGNISED_ROWS, 2)
        one_row = [('Yellow cards', '2', '4'), ('Related', 'x', 'y')]
        self.assertIsNone(_best_table([one_row]))

    def test_no_tables_returns_none(self):
        self.assertIsNone(_best_table([]))


class TestScorelineNotMistakenForAStat(unittest.TestCase):
    """
    Regression coverage for a live failure: the raw sample reported back was
    ('3', '-', '0') -- the scoreline, not a stat row. It got there because
    the old list-extraction filter matched any element whose class contained
    the substring 'stat'/'Stat', and a class like "MatchStatus" contains
    "Stat" as a literal substring. Filtering is now done purely by whether a
    row's text resolves to a known label, with no class-name guessing.
    """

    def test_scoreline_alone_never_recognised(self):
        self.assertIsNone(orient_triple('3', '-', '0'))

    def test_scoreline_plus_noise_does_not_clear_the_threshold(self):
        noise_table = [('3', '-', '0'), ('Related', 'Read more', 'A story')]
        self.assertIsNone(_best_table([noise_table]))

    def test_scoreline_alongside_real_stats_is_simply_ignored(self):
        """
        The scoreline can appear on the same page as real stats without
        polluting them -- it just never resolves to a label, same as any
        other unrecognised row.
        """
        table = [
            ('3', '-', '0'),
            ('Possession %', '62', '38'),
            ('Shots', '17', '9'),
        ]
        result = _best_table([table])
        self.assertIsNotNone(result)
        self.assertNotIn(('3', '-', '0'), result)
        self.assertIn(('Possession %', '62', '38'), result)


class TestStatsContentReady(unittest.TestCase):
    """
    Regression coverage for a live failure reported after the scoreline fix
    above: "tab opened; 2 row(s) read across 0 table(s) plus list layout,
    but none matched a known stat label. raw sample: ('2', '-', '0');
    ('Sat 22 Aug', 'Hill Dickinson Stadium', 'Paul Tierney')".

    The poll's default probe used to be `bool(_raw_table_rows(driver)) or
    bool(_raw_list_rows(driver))` -- true as soon as ANY 3-part group
    existed, recognised or not. But the scoreline and the kickoff/venue/
    referee line are themselves valid 3-part groups, and both render on the
    match page before the Stats-tab panel does. So the poll was satisfied by
    that furniture on its first check and stopped immediately, handing
    scrape_match_stats() a DOM that had the scoreline and match info but not
    yet the real stats table. _stats_content_ready() is the fix: it applies
    the same MIN_RECOGNISED_ROWS recognition test the final decision already
    uses, so furniture that never resolves to a known label does not end the
    poll early.
    """

    class _FakeElement:
        def __init__(self, text):
            self.text = text

    class _FakeDriver:
        def __init__(self, list_texts=()):
            self._list_texts = list_texts

        def find_elements(self, by, xpath):
            if xpath == '//table':
                return []
            if 'self::li or self::div' in xpath:
                return [TestStatsContentReady._FakeElement(t)
                        for t in self._list_texts]
            return []

    def test_scoreline_and_match_info_alone_are_not_ready(self):
        driver = self._FakeDriver(list_texts=[
            '2\n-\n0',
            'Sat 22 Aug\nHill Dickinson Stadium\nPaul Tierney',
        ])
        self.assertFalse(_stats_content_ready(driver))

    def test_real_stats_panel_is_ready(self):
        driver = self._FakeDriver(list_texts=[
            '2\n-\n0',
            'Possession %\n62\n38',
            'Shots\n17\n9',
        ])
        self.assertTrue(_stats_content_ready(driver))

    def test_completely_empty_page_is_not_ready(self):
        self.assertFalse(_stats_content_ready(self._FakeDriver()))


class TestWaitForStatsContent(unittest.TestCase):
    """
    A fixed sleep after the tab click is a guess about render timing; a
    report with 0 tables and only the scoreline found is consistent with the
    real panel not having rendered by the time a fixed sleep ran out.

    The polling loop is tested via the injectable `probe`, rather than a
    faked Selenium element tree -- that keeps this independent of Selenium's
    internal traversal, and it is the loop's stop-early-or-use-the-budget
    behaviour that matters here, not DOM mechanics already covered elsewhere.
    """

    def test_returns_true_as_soon_as_content_appears(self):
        calls = {'n': 0}

        def probe():
            calls['n'] += 1
            return calls['n'] >= 3  # "appears" on the 3rd check

        elapsed = []
        found = _wait_for_stats_content(
            driver=None, timeout=5, poll_interval=0.1,
            sleep=lambda s: elapsed.append(s), probe=probe)

        self.assertTrue(found)
        # Stopped polling once content appeared, not at the full timeout.
        self.assertLess(sum(elapsed), 5)
        self.assertEqual(calls['n'], 3)

    def test_gives_up_at_timeout_for_a_genuinely_empty_page(self):
        found = _wait_for_stats_content(
            driver=None, timeout=1, poll_interval=0.5,
            sleep=lambda s: None, probe=lambda: False)
        self.assertFalse(found)

    def test_default_probe_checks_the_real_dom_when_not_injected(self):
        """The injectable seam must not change production behaviour."""
        class EmptyDriver:
            def find_elements(self, by, xpath):
                return []

        found = _wait_for_stats_content(
            EmptyDriver(), timeout=0.1, poll_interval=0.05,
            sleep=lambda s: None)
        self.assertFalse(found)


class TestDiagnoseNoMatch(unittest.TestCase):
    """
    The error text is the interface for diagnosing a live failure without
    separate access to the page, so its shape is worth locking down. Pure
    function over already-collected data -- no Selenium involved.

    `tab_status` is a free-form string (what open_stats_tab() reports), not a
    bool -- it needs to be able to say more than opened/not-opened, e.g. that
    the tab was found but blocked by something covering it.
    """

    def test_reports_tab_opened(self):
        msg = _diagnose_no_match('tab opened', tables=[],
                                 list_rows=[('3', '-', '0')])
        self.assertIn('tab opened', msg)

    def test_reports_tab_not_found(self):
        msg = _diagnose_no_match('tab not found in the DOM', tables=[], list_rows=[])
        self.assertIn('tab not found', msg)

    def test_reports_tab_blocked_by_something_covering_it(self):
        """
        The scenario this whole distinction exists for: the tab is present
        but an undismissed cookie banner (or similar) is in the way.
        """
        msg = _diagnose_no_match(
            'tab located but the click was intercepted -- it is covered by '
            'another element, e.g. an undismissed cookie banner',
            tables=[], list_rows=[('3', '-', '0')])
        self.assertIn('intercepted', msg)
        self.assertIn('cookie banner', msg)

    def test_raw_sample_included_when_rows_found_but_unrecognised(self):
        msg = _diagnose_no_match('tab opened', tables=[],
                                 list_rows=[('3', '-', '0')])
        self.assertIn("('3', '-', '0')", msg)

    def test_row_and_table_counts_reported(self):
        tables = [[('Related', 'x', 'y'), ('Related', 'a', 'b')]]
        msg = _diagnose_no_match('tab opened', tables=tables,
                                 list_rows=[('3', '-', '0')])
        self.assertIn('3 row(s)', msg)
        self.assertIn('1 table(s)', msg)

    def test_completely_empty_page_gets_its_own_message(self):
        msg = _diagnose_no_match('tab not found in the DOM', tables=[], list_rows=[])
        self.assertIn('no stat rows found', msg)


if __name__ == '__main__':
    unittest.main(verbosity=2)
