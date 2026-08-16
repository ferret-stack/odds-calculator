"""
End-to-end pipeline test against a synthetic data directory.

Covers the wiring rather than the maths: that the stages connect, that the
sanity checks reach the output, that qualitative context is attached without
touching the pricing, and that --dry-run really leaves the ledger alone.
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.bankroll import Ledger
from pipeline.run_pipeline import run


def write(path, payload):
    Path(path).write_text(json.dumps(payload))


class PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data = Path(self.tmp.name)
        (self.data / 'qualitative').mkdir()

        write(self.data / 'current_elo.json', {
            'Arsenal': {'elo': 2009, 'rank': 1},
            'Everton': {'elo': 1784, 'rank': 13},
            'Leeds': {'elo': 1763, 'rank': 15},
        })

        # Band 5 (diff 225) and band 1 (diff 21).
        write(self.data / 'elo_bands.json', [
            {'band': b, 'range': '-', 'total_games': 100,
             'evenly_rated_games': 0,
             'stronger_win_pct': 0.40 + b * 0.03,
             'draw_pct': 0.26,
             'weaker_win_pct': max(0.02, 0.34 - b * 0.03),
             'avg_booking_points': 40,
             'over_05_pct': 0.9, 'over_15_pct': 0.75, 'over_25_pct': 0.5,
             'over_35_pct': 0.25, 'over_45_pct': 0.1, 'btts_pct': 0.5}
            for b in range(1, 11)
        ])

        write(self.data / 'matches_data.json', [
            {'date': '2026-09-16', 'home_team': 'Arsenal', 'away_team': 'Leeds',
             'home_goals': 2, 'away_goals': 0},
            {'date': '2026-09-12', 'home_team': 'Everton', 'away_team': 'Arsenal',
             'home_goals': 1, 'away_goals': 1},
        ])

        write(self.data / 'upcoming_fixtures.json', [{
            'home_team': 'Arsenal', 'away_team': 'Everton',
            'date': '2026-09-20', 'time': '15:00', 'game_id': 'test1',
            'bookmaker_odds': {'home': 1.90, 'draw': 4.00, 'away': 6.00},
        }])

        write(self.data / 'qualitative' / 'manager_styles.json',
              {'Arsenal': 'high press'})
        write(self.data / 'qualitative' / 'team_news.json',
              {'Arsenal': 'Saka doubtful'})
        write(self.data / 'qualitative' / 'formations.json',
              {'Arsenal': '4-3-3'})
        write(self.data / 'cup_fixtures.json', {'teams': {'Arsenal': [
            {'date': '2026-09-17', 'competition': 'Carabao Cup',
             'opponent': 'Brighton'}]}})
        write(self.data / 'pipeline_config.json',
              {'starting_bankroll': 1000.0, 'low_confidence_bands': [9, 10]})

    def tearDown(self):
        self.tmp.cleanup()

    def _run(self, **kwargs):
        return run(data_dir=self.data, **kwargs)


class TestPipelineRuns(PipelineTestCase):
    def test_produces_a_report(self):
        report = self._run(dry_run=True)
        self.assertTrue((self.data / 'pipeline_report.json').exists())
        self.assertEqual(report['totals']['priced'], 3)  # home/draw/away

    def test_staking_rule_recorded_in_report(self):
        rule = self._run(dry_run=True)['staking_rule']
        self.assertIn('Quarter-Kelly (0.25)', rule['standard'])
        self.assertIn('Eighth-Kelly (0.125)', rule['hedge_or_low_confidence'])
        self.assertEqual(rule['minimum_edge'], '+5% EV')
        self.assertIn('not Half-Kelly', rule['note'])

    def test_every_placed_bet_uses_a_sanctioned_multiplier(self):
        report = self._run(dry_run=True)
        for sel in report['selections']:
            if sel['bet']:
                self.assertIn(sel['kelly_multiplier'], (0.25, 0.125))

    def test_no_bet_below_the_ev_floor(self):
        report = self._run(dry_run=True)
        for sel in report['selections']:
            if sel['bet']:
                self.assertGreaterEqual(sel['edge_pct'], 5.0)

    def test_stake_equals_bankroll_times_fraction(self):
        report = self._run(dry_run=True)
        bankroll = report['bankroll']['staking_bankroll']
        for sel in report['selections']:
            if sel['bet']:
                self.assertAlmostEqual(
                    sel['stake'], round(bankroll * sel['stake_fraction'], 2),
                    places=1)


class TestQualitativeAttached(PipelineTestCase):
    def test_context_present_for_each_fixture(self):
        contexts = self._run(dry_run=True)['qualitative_context']
        self.assertIn('Arsenal v Everton', contexts)
        home = contexts['Arsenal v Everton']['home']
        self.assertEqual(home['manager_style'], 'high press')
        self.assertEqual(home['team_news'], 'Saka doubtful')
        self.assertEqual(home['formation'], '4-3-3')

    def test_congestion_counts_pl_games_only(self):
        home = self._run(dry_run=True)['qualitative_context'][
            'Arsenal v Everton']['home']
        # Two PL games in the window; the 17 Sep Carabao tie must not count.
        self.assertEqual(home['congestion']['value'], 2)
        self.assertEqual(home['congestion']['basis'], 'PL-only proxy')

    def test_cup_tie_present_but_marked_narrative(self):
        home = self._run(dry_run=True)['qualitative_context'][
            'Arsenal v Everton']['home']
        self.assertEqual(len(home['cup_fixtures']['recent']), 1)
        self.assertIn('narrative context only', home['cup_fixtures']['note'])

    def test_congestion_caveat_in_report_top_level(self):
        report = self._run(dry_run=True)
        self.assertIn('PL-only proxy', report['congestion_signal'])
        self.assertIn('not a complete fatigue measure', report['congestion_signal'])


class TestDryRun(PipelineTestCase):
    def test_dry_run_writes_no_bets(self):
        self._run(dry_run=True)
        self.assertFalse((self.data / 'bankroll.json').exists())

    def test_live_run_writes_bets(self):
        report = self._run(dry_run=False)
        self.assertTrue((self.data / 'bankroll.json').exists())
        ledger = Ledger(path=self.data / 'bankroll.json')
        self.assertEqual(len(ledger.bets), report['totals']['bets_placed'])


class TestUnratedTeamsSkipped(PipelineTestCase):
    def test_fixture_with_unknown_team_is_skipped_not_priced(self):
        write(self.data / 'upcoming_fixtures.json', [{
            'home_team': 'Arsenal', 'away_team': 'Barcelona',
            'date': '2026-09-20', 'time': '15:00', 'game_id': 'x',
            'bookmaker_odds': {'home': 1.90, 'draw': 4.00, 'away': 6.00},
        }])
        report = self._run(dry_run=True)
        self.assertEqual(report['totals']['priced'], 0)
        self.assertEqual(len(report['skipped_fixtures']), 1)
        self.assertIn('no ELO rating', report['skipped_fixtures'][0]['reason'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
