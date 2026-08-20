"""
Tests for the staking rule.

The rule under test, in full:
    Quarter-Kelly (0.25) is a ceiling for standard +EV plays.
    Eighth-Kelly (0.125) for hedges and lower-confidence plays.
    Act only on +5% EV or better.
    NOT Half-Kelly.
"""

import inspect
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import staking
from pipeline.staking import (
    EIGHTH_KELLY, HEDGE, LOW_CONFIDENCE, MIN_EV, QUARTER_KELLY, STANDARD,
    apply_sanity_checks, expected_value, full_kelly_fraction, size_bet,
)


class TestRuleConstants(unittest.TestCase):
    def test_multipliers_are_quarter_and_eighth(self):
        self.assertEqual(QUARTER_KELLY, 0.25)
        self.assertEqual(EIGHTH_KELLY, 0.125)

    def test_minimum_edge_is_five_percent(self):
        self.assertEqual(MIN_EV, 0.05)

    def test_half_kelly_appears_nowhere(self):
        """
        Older repo docs reference Half-Kelly. It must not survive in code.

        Checked against the module's actual constants rather than its source
        text -- the prose explaining why 0.5 is wrong legitimately contains
        the string "0.5".
        """
        self.assertNotIn(0.5, set(staking.MULTIPLIER.values()))

        numeric_constants = {
            name: value for name, value in vars(staking).items()
            if isinstance(value, float) and name.isupper()
        }
        self.assertNotIn(0.5, numeric_constants.values(), numeric_constants)
        self.assertFalse([n for n in numeric_constants if 'HALF' in n])

    def test_every_confidence_class_maps_to_a_known_multiplier(self):
        self.assertEqual(set(staking.MULTIPLIER.values()),
                         {QUARTER_KELLY, EIGHTH_KELLY})


class TestKellyMaths(unittest.TestCase):
    def test_expected_value(self):
        # p=0.55, d=2.10 -> 0.55*2.10 - 1 = 0.155
        self.assertAlmostEqual(expected_value(0.55, 2.10), 0.155, places=10)

    def test_full_kelly_formula(self):
        # f* = (p*d - 1)/(d - 1) = 0.155 / 1.10
        self.assertAlmostEqual(full_kelly_fraction(0.55, 2.10),
                               0.155 / 1.10, places=10)

    def test_kelly_equals_ev_over_net_odds(self):
        for p, d in ((0.55, 2.10), (0.34, 3.40), (0.72, 1.55)):
            self.assertAlmostEqual(full_kelly_fraction(p, d),
                                   expected_value(p, d) / (d - 1), places=12)

    def test_negative_edge_gives_zero_not_negative(self):
        self.assertEqual(full_kelly_fraction(0.40, 2.00), 0.0)

    def test_fair_odds_give_zero_kelly(self):
        self.assertAlmostEqual(full_kelly_fraction(0.50, 2.00), 0.0, places=12)

    def test_invalid_inputs_rejected(self):
        with self.assertRaises(ValueError):
            expected_value(1.4, 2.0)
        with self.assertRaises(ValueError):
            expected_value(0.5, 1.0)


class TestSizeBet(unittest.TestCase):
    def test_standard_play_uses_quarter_kelly(self):
        d = size_bet('A v B', '1x2', 'home', 0.55, 2.10, 1000.0, STANDARD)
        self.assertTrue(d.bet)
        self.assertEqual(d.multiplier, QUARTER_KELLY)
        self.assertAlmostEqual(d.stake_fraction, 0.25 * (0.155 / 1.10), places=12)

    def test_hedge_uses_eighth_kelly(self):
        d = size_bet('A v B', '1x2', 'home', 0.55, 2.10, 1000.0, HEDGE)
        self.assertEqual(d.multiplier, EIGHTH_KELLY)

    def test_low_confidence_uses_eighth_kelly(self):
        d = size_bet('A v B', '1x2', 'home', 0.55, 2.10, 1000.0, LOW_CONFIDENCE)
        self.assertEqual(d.multiplier, EIGHTH_KELLY)

    def test_hedge_is_exactly_half_the_standard_stake(self):
        """Same selection, different class: 0.125 is half of 0.25."""
        std = size_bet('A v B', '1x2', 'home', 0.55, 2.10, 1000.0, STANDARD)
        hedge = size_bet('A v B', '1x2', 'home', 0.55, 2.10, 1000.0, HEDGE)
        self.assertAlmostEqual(hedge.stake, std.stake / 2, places=2)

    def test_quarter_kelly_is_a_ceiling_never_exceeded(self):
        """No input may produce a stake above 0.25 x full Kelly."""
        for p in (0.30, 0.5, 0.7, 0.9, 0.99):
            for d_odds in (1.2, 2.0, 3.5, 10.0):
                dec = size_bet('f', 'm', 's', p, d_odds, 1000.0, STANDARD)
                ceiling = QUARTER_KELLY * full_kelly_fraction(p, d_odds)
                self.assertLessEqual(dec.stake_fraction, ceiling + 1e-12)

    def test_edge_below_five_percent_rejected(self):
        # p=0.50, d=2.08 -> EV = +4.0%
        d = size_bet('A v B', '1x2', 'home', 0.50, 2.08, 1000.0)
        self.assertFalse(d.bet)
        self.assertEqual(d.stake, 0.0)
        self.assertIn('below', d.reason)

    def test_edge_exactly_five_percent_is_accepted(self):
        # p=0.525, d=2.00 -> EV = exactly +5%
        d = size_bet('A v B', '1x2', 'home', 0.525, 2.00, 1000.0)
        self.assertTrue(d.bet)

    def test_negative_edge_rejected(self):
        self.assertFalse(size_bet('A v B', '1x2', 'home', 0.40, 2.00, 1000.0).bet)

    def test_zero_bankroll_places_nothing(self):
        d = size_bet('A v B', '1x2', 'home', 0.55, 2.10, 0.0)
        self.assertFalse(d.bet)

    def test_stake_scales_linearly_with_bankroll(self):
        """
        Compared on stake_fraction, which is exact. `stake` is rounded to
        pence, so 5 x a rounded stake is not the rounded 5x stake.
        """
        small = size_bet('f', 'm', 's', 0.55, 2.10, 1000.0)
        big = size_bet('f', 'm', 's', 0.55, 2.10, 5000.0)
        self.assertAlmostEqual(big.stake_fraction, small.stake_fraction, places=12)
        self.assertAlmostEqual(big.stake, small.stake * 5, places=1)

    def test_unknown_confidence_class_rejected(self):
        with self.assertRaises(ValueError):
            size_bet('f', 'm', 's', 0.55, 2.10, 1000.0, 'half_kelly')

    def test_worked_example_by_hand(self):
        """
        Fully hand-checkable:
          p = 0.55, d = 2.10, bankroll = 1000
          EV     = 0.55*2.10 - 1        = 0.155      (+15.5%, clears +5%)
          f*     = 0.155 / 1.10         = 0.1409091
          stake% = 0.25 * 0.1409091     = 0.0352273
          stake  = 1000 * 0.0352273     = 35.23
        """
        d = size_bet('Arsenal v Everton', '1x2', 'home', 0.55, 2.10, 1000.0)
        self.assertAlmostEqual(d.expected_value, 0.155, places=10)
        self.assertAlmostEqual(d.full_kelly, 0.14090909, places=8)
        self.assertAlmostEqual(d.stake_fraction, 0.03522727, places=8)
        self.assertEqual(d.stake, 35.23)


class TestSanityChecks(unittest.TestCase):
    def _bet(self, fixture, selection, market='m', p=0.55, odds=2.10):
        return size_bet(fixture, market, selection, p, odds, 1000.0)

    def test_two_ev_in_same_market_blocks_both(self):
        """Not a pair of bets -- a signal to re-check the model."""
        decisions = [self._bet('A v B', 'home'), self._bet('A v B', 'away')]
        decisions, findings = apply_sanity_checks(decisions)

        self.assertTrue(all(not d.bet for d in decisions))
        self.assertTrue(all(d.stake == 0.0 for d in decisions))
        kinds = {f.kind for f in findings}
        self.assertIn('same_market_multiple_ev', kinds)

    def test_same_market_finding_says_recheck_the_model(self):
        decisions = [self._bet('A v B', 'over_25'), self._bet('A v B', 'under_25')]
        _, findings = apply_sanity_checks(decisions)
        self.assertIn('re-check the model', findings[0].detail)

    def test_correlated_cross_market_bets_blocked_without_explanation(self):
        decisions = [self._bet('A v B', 'home'), self._bet('A v B', 'over_25')]
        decisions, findings = apply_sanity_checks(decisions)

        self.assertTrue(all(not d.bet for d in decisions))
        self.assertIn('correlated_bets_unexplained', {f.kind for f in findings})

    def test_correlated_bets_allowed_when_explained(self):
        decisions = [self._bet('A v B', 'home'), self._bet('A v B', 'over_25')]
        explanations = {
            frozenset({'home', 'over_25'}):
                'favourite chases an early goal; correlation accepted knowingly'
        }
        decisions, findings = apply_sanity_checks(decisions, explanations)

        self.assertTrue(all(d.bet for d in decisions))
        self.assertIn('correlation_explained', {f.kind for f in findings})

    def test_single_bet_per_fixture_is_untouched(self):
        decisions = [self._bet('A v B', 'home'), self._bet('C v D', 'away')]
        decisions, findings = apply_sanity_checks(decisions)
        self.assertTrue(all(d.bet for d in decisions))
        self.assertEqual([f for f in findings if f.blocks], [])

    def test_implausible_edge_is_flagged_but_not_blocked(self):
        """Advisory only -- the operator's rules do not call for blocking it."""
        decisions = [self._bet('A v B', 'home', p=0.60, odds=2.50)]  # +50% EV
        decisions, findings = apply_sanity_checks(decisions)

        self.assertTrue(decisions[0].bet)
        self.assertGreater(decisions[0].stake, 0)
        flagged = [f for f in findings if f.kind == 'implausible_edge']
        self.assertEqual(len(flagged), 1)
        self.assertFalse(flagged[0].blocks)

    def test_ordinary_edge_not_flagged(self):
        decisions = [self._bet('A v B', 'home', p=0.55, odds=2.10)]  # +15.5%
        _, findings = apply_sanity_checks(decisions)
        self.assertEqual([f for f in findings if f.kind == 'implausible_edge'], [])

    def test_blocked_bet_stays_visible_with_flags(self):
        decisions = [self._bet('A v B', 'home'), self._bet('A v B', 'away')]
        decisions, _ = apply_sanity_checks(decisions)
        self.assertEqual(len(decisions), 2)
        self.assertTrue(all(d.flags for d in decisions))


if __name__ == '__main__':
    unittest.main(verbosity=2)
