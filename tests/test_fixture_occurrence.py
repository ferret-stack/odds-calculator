"""Tests for the cup/European fixture-occurrence parser."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.fixture_occurrence import (
    is_european, merge, normalise_competition, parse_date, parse_fixture_rows,
)


class TestNormaliseCompetition(unittest.TestCase):
    def test_in_scope(self):
        cases = {
            'Emirates FA Cup': 'FA Cup',
            'FA CUP': 'FA Cup',
            'Carabao Cup': 'Carabao Cup',
            'EFL Cup': 'Carabao Cup',
            'League Cup': 'Carabao Cup',
            'UEFA Champions League': 'UEFA Champions League',
            'Europa League': 'UEFA Europa League',
            'UEFA Conference League': 'UEFA Conference League',
        }
        for raw, expected in cases.items():
            self.assertEqual(normalise_competition(raw), expected, raw)

    def test_premier_league_excluded(self):
        """PL occurrence lives in matches_data.json; including it double-counts."""
        self.assertIsNone(normalise_competition('Premier League'))

    def test_other_exclusions(self):
        for raw in ('Community Shield', 'Club Friendly', 'UEFA Super Cup',
                    'FIFA Club World Cup', 'Championship', 'EFL Trophy'):
            self.assertIsNone(normalise_competition(raw), raw)

    def test_unknown_is_dropped(self):
        self.assertIsNone(normalise_competition('Some Invitational'))
        self.assertIsNone(normalise_competition(''))
        self.assertIsNone(normalise_competition(None))

    def test_european_grouping(self):
        self.assertTrue(is_european('UEFA Europa League'))
        self.assertFalse(is_european('FA Cup'))


class TestParseDate(unittest.TestCase):
    def test_explicit_formats(self):
        self.assertEqual(parse_date('3 January 2027'), '2027-01-03')
        self.assertEqual(parse_date('03/01/2027'), '2027-01-03')
        self.assertEqual(parse_date('2027-01-03'), '2027-01-03')

    def test_weekday_and_ordinal_stripped(self):
        self.assertEqual(parse_date('Sat 3rd January 2027'), '2027-01-03')
        self.assertEqual(parse_date('Tuesday, 23rd Sep 2026'), '2026-09-23')

    def test_year_inferred_from_season(self):
        """Aug-Dec belong to the opening year, Jan-Jul to the next."""
        self.assertEqual(parse_date('23 September', 2026), '2026-09-23')
        self.assertEqual(parse_date('3 January', 2026), '2027-01-03')

    def test_unparseable_dropped_not_guessed(self):
        self.assertIsNone(parse_date('TBC'))
        self.assertIsNone(parse_date(''))
        self.assertIsNone(parse_date(None))


class TestParseFixtureRows(unittest.TestCase):
    ROWS = [
        ('Tue 23 Sep 2026', 'Carabao Cup', 'Brighton'),
        ('Sat 10 Jan 2027', 'Emirates FA Cup', 'Hull City'),
        ('Wed 17 Sep 2026', 'UEFA Champions League', 'Real Madrid'),
        ('Sat 20 Sep 2026', 'Premier League', 'Everton'),
        ('Sun 9 Aug 2026', 'Community Shield', 'Liverpool'),
    ]

    def test_only_in_scope_kept(self):
        out = parse_fixture_rows(self.ROWS, 'Arsenal', 2026)
        self.assertEqual(len(out), 3)
        comps = {f['competition'] for f in out}
        self.assertEqual(comps, {'Carabao Cup', 'FA Cup', 'UEFA Champions League'})

    def test_exactly_three_fields(self):
        for fixture in parse_fixture_rows(self.ROWS, 'Arsenal', 2026):
            self.assertEqual(set(fixture), {'date', 'competition', 'opponent'})

    def test_sorted_by_date(self):
        out = parse_fixture_rows(self.ROWS, 'Arsenal', 2026)
        self.assertEqual([f['date'] for f in out], sorted(f['date'] for f in out))

    def test_venue_marker_stripped(self):
        out = parse_fixture_rows(
            [('Tue 23 Sep 2026', 'Carabao Cup', 'Brighton (A)')], 'Arsenal', 2026)
        self.assertEqual(out[0]['opponent'], 'Brighton')

    def test_duplicates_removed(self):
        out = parse_fixture_rows(self.ROWS + self.ROWS, 'Arsenal', 2026)
        self.assertEqual(len(out), 3)

    def test_self_as_opponent_dropped(self):
        out = parse_fixture_rows(
            [('Tue 23 Sep 2026', 'Carabao Cup', 'Arsenal')], 'Arsenal', 2026)
        self.assertEqual(out, [])

    def test_unparseable_date_dropped(self):
        out = parse_fixture_rows(
            [('TBC', 'FA Cup', 'Hull City')], 'Arsenal', 2026)
        self.assertEqual(out, [])

    def test_standardise_applied_to_opponent(self):
        out = parse_fixture_rows(
            [('Tue 23 Sep 2026', 'Carabao Cup', 'Tottenham Hotspur')],
            'Arsenal', 2026, standardise=lambda n: 'Spurs' if 'Tottenham' in n else n)
        self.assertEqual(out[0]['opponent'], 'Spurs')


class TestMerge(unittest.TestCase):
    def test_manual_entries_added(self):
        scraped = [{'date': '2026-09-23', 'competition': 'Carabao Cup',
                    'opponent': 'Brighton'}]
        manual = {'Arsenal': [{'date': '10 January 2027', 'competition': 'FA Cup',
                               'opponent': 'Hull City'}]}
        out = merge(scraped, manual, 'Arsenal')
        self.assertEqual(len(out), 2)
        self.assertEqual(out[1]['date'], '2027-01-10')

    def test_manual_duplicate_not_doubled(self):
        scraped = [{'date': '2026-09-23', 'competition': 'Carabao Cup',
                    'opponent': 'Brighton'}]
        manual = {'Arsenal': [{'date': '2026-09-23', 'competition': 'Carabao Cup',
                               'opponent': 'Brighton'}]}
        self.assertEqual(len(merge(scraped, manual, 'Arsenal')), 1)

    def test_incomplete_manual_entry_skipped(self):
        manual = {'Arsenal': [{'date': '2026-09-23', 'competition': 'FA Cup'}]}
        self.assertEqual(merge([], manual, 'Arsenal'), [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
