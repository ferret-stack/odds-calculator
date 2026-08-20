"""
Tests for the qualitative layer.

The congestion signal is the piece with a precise definition to hold to:
PL games in the trailing 14 days, numeric, PL-only, never including cups.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.qualitative import (
    CONGESTION_WINDOW_DAYS, congestion_signal, cup_context, fixture_context,
    summarise,
)


def match(date, home, away):
    return {'date': date, 'home_team': home, 'away_team': away}


class TestCongestionSignal(unittest.TestCase):
    MATCHES = [
        match('2026-09-01', 'Arsenal', 'Everton'),     # 19 days before: outside
        match('2026-09-12', 'Chelsea', 'Arsenal'),     #  8 days before: inside
        match('2026-09-16', 'Arsenal', 'Leeds'),       #  4 days before: inside
        match('2026-09-18', 'Fulham', 'Arsenal'),      #  2 days before: inside
        match('2026-09-20', 'Arsenal', 'Spurs'),       # the fixture date
        match('2026-09-25', 'Arsenal', 'Wolves'),      # after: outside
        match('2026-09-14', 'Chelsea', 'Leeds'),       # not Arsenal
    ]

    def test_counts_only_trailing_window(self):
        signal = congestion_signal('Arsenal', self.MATCHES, '2026-09-20')
        self.assertEqual(signal['value'], 3)

    def test_window_is_fourteen_days(self):
        self.assertEqual(CONGESTION_WINDOW_DAYS, 14)
        self.assertEqual(
            congestion_signal('Arsenal', self.MATCHES, '2026-09-20')['window_days'],
            14)

    def test_fixture_date_itself_excluded(self):
        """A game on the day is not 'in the trailing window'."""
        played = congestion_signal('Arsenal', self.MATCHES, '2026-09-20')['matches']
        self.assertNotIn('2026-09-20', [m['date'] for m in played])

    def test_boundary_day_excluded(self):
        """Exactly 14 days before is outside, so 14 days means 14."""
        boundary = [match('2026-09-06', 'Arsenal', 'Everton')]
        self.assertEqual(
            congestion_signal('Arsenal', boundary, '2026-09-20')['value'], 0)

    def test_counts_home_and_away(self):
        played = congestion_signal('Arsenal', self.MATCHES, '2026-09-20')['matches']
        self.assertEqual([m['opponent'] for m in played],
                         ['Chelsea', 'Leeds', 'Fulham'])

    def test_other_teams_not_counted(self):
        self.assertEqual(
            congestion_signal('Bournemouth', self.MATCHES, '2026-09-20')['value'], 0)

    def test_value_is_numeric(self):
        self.assertIsInstance(
            congestion_signal('Arsenal', self.MATCHES, '2026-09-20')['value'], int)

    def test_labelled_as_pl_only_proxy(self):
        signal = congestion_signal('Arsenal', self.MATCHES, '2026-09-20')
        self.assertEqual(signal['basis'], 'PL-only proxy')
        caveat = signal['caveat'].lower()
        self.assertIn('pl-only proxy', caveat)
        self.assertIn('not a complete fatigue measure', caveat)
        for competition in ('fa cup', 'carabao', 'european'):
            self.assertIn(competition, caveat)

    def test_malformed_dates_skipped_not_fatal(self):
        bad = self.MATCHES + [{'date': 'nonsense', 'home_team': 'Arsenal',
                               'away_team': 'Leeds'}]
        self.assertEqual(congestion_signal('Arsenal', bad, '2026-09-20')['value'], 3)


class TestCupsStaySeparate(unittest.TestCase):
    CUPS = {'teams': {'Arsenal': [
        {'date': '2026-09-16', 'competition': 'Carabao Cup', 'opponent': 'Brighton'},
        {'date': '2026-09-23', 'competition': 'FA Cup', 'opponent': 'Hull City'},
    ]}}

    def test_cup_ties_never_enter_the_congestion_number(self):
        """The whole point of keeping Task 2's output separate."""
        signal = congestion_signal('Arsenal', [], '2026-09-20')
        self.assertEqual(signal['value'], 0)

        pl_only = congestion_signal(
            'Arsenal', [match('2026-09-16', 'Arsenal', 'Leeds')], '2026-09-20')
        self.assertEqual(pl_only['value'], 1)  # the PL game, not the cup tie

    def test_cup_context_split_into_recent_and_upcoming(self):
        ctx = cup_context('Arsenal', self.CUPS, '2026-09-20')
        self.assertEqual([f['date'] for f in ctx['recent']], ['2026-09-16'])
        self.assertEqual([f['date'] for f in ctx['upcoming']], ['2026-09-23'])

    def test_cup_context_marked_narrative_only(self):
        ctx = cup_context('Arsenal', self.CUPS, '2026-09-20')
        self.assertIn('narrative context only', ctx['note'])


class TestFixtureContext(unittest.TestCase):
    def _context(self):
        fixture = {'home_team': 'Arsenal', 'away_team': 'Everton',
                   'date': '2026-09-20'}
        qualitative = {
            'manager_styles': {'Arsenal': 'high press, inverted full-backs'},
            'team_news': {'Arsenal': 'Saka doubtful'},
            'formations': {'Arsenal': '4-3-3'},
        }
        cups = {'teams': {'Arsenal': [
            {'date': '2026-09-16', 'competition': 'Carabao Cup',
             'opponent': 'Brighton'}]}}
        matches = [match('2026-09-16', 'Arsenal', 'Leeds')]
        return fixture_context(fixture, matches, qualitative, cups)

    def test_all_four_inputs_present(self):
        home = self._context()['home']
        self.assertEqual(home['manager_style'], 'high press, inverted full-backs')
        self.assertEqual(home['team_news'], 'Saka doubtful')
        self.assertEqual(home['formation'], '4-3-3')
        self.assertEqual(home['congestion']['value'], 1)

    def test_missing_data_is_none_not_invented(self):
        away = self._context()['away']
        self.assertIsNone(away['manager_style'])
        self.assertIsNone(away['team_news'])
        self.assertIsNone(away['formation'])

    def test_caveat_travels_with_the_context(self):
        self.assertIn('PL-only proxy',
                      self._context()['congestion_signal_definition'])

    def test_summary_labels_congestion_and_marks_cups_narrative(self):
        line = summarise(self._context(), 'home')
        self.assertIn('congestion 1 PL game(s)/14d (PL-only proxy)', line)
        self.assertIn('[narrative only]', line)


if __name__ == '__main__':
    unittest.main(verbosity=2)
