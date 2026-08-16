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
    booking_points, build_result, empty_result, normalise_label,
    parse_stat_rows, parse_stat_value,
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
