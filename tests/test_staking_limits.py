"""
Exposure caps, the implausible-edge fix, and band confidence -- checked
against the real numbers this week's and last week's portfolios were built
from, not against invented ones.

The figures come from data/bankroll.json:

  MW2, staking bankroll 1016.75 (1000 start, +16.75 realised from MW1)
    00009  Leeds v Brentford, away @ 2.70, p=0.4449, EV +20.12%, staked 30.09

  MW3, staking bankroll 989.11
    00014  Nott'm Forest v Spurs, home @ 2.51, p=0.5395, EV +35.41%
    00015  Arsenal v Chelsea,     home @ 1.69, p=0.6262, EV  +5.83%

Forest and Arsenal are the pair the ticket names as the check, and between
them they cover both sides of every rule here: Forest trips the
implausible-edge threshold and Arsenal does not; Forest's pre-fix stake
breaches the per-bet cap and its post-fix stake does not; Arsenal never
approaches either.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.staking import (
    EIGHTH_KELLY, LARGE_EDGE, QUARTER_KELLY, STANDARD, apply_stake_cap,
    check_weekly_exposure, size_bet, staking_limits,
)

# --- this week ------------------------------------------------------------
MW3_BANKROLL = 989.11
FOREST = dict(fixture="Nott'm Forest v Spurs", market='1x2', selection='home',
              probability=0.5395, odds=2.51)
ARSENAL = dict(fixture='Arsenal v Chelsea', market='1x2', selection='home',
               probability=0.6262, odds=1.69)

# --- last week ------------------------------------------------------------
MW2_BANKROLL = 1016.75
BRENTFORD_MW2 = dict(fixture='Leeds v Brentford', market='1x2',
                     selection='away', probability=0.4449, odds=2.70)
# The whole MW2 book, in ledger order: (selection, p, odds).
MW2_BOOK = [
    ("Liverpool v Nott'm Forest", 'away', 0.1739, 6.19),
    ('Bournemouth v Everton', 'away', 0.3067, 3.46),
    ('Spurs v Newcastle', 'away', 0.4449, 2.99),
    ('Leeds v Brentford', 'away', 0.4449, 2.70),
    ('Chelsea v Brighton', 'away', 0.3615, 3.72),
    ('Sunderland v Fulham', 'away', 0.4449, 2.91),
]


def quiet(_message):
    """Swallow the clamp log line; the ClampEvent is what is asserted."""


class TestConfiguredLimits(unittest.TestCase):
    """The caps are configuration, not literals at the call site."""

    def test_defaults_are_three_and_twelve_percent(self):
        self.assertEqual(staking_limits(), (0.03, 0.12))

    def test_config_overrides_the_defaults(self):
        config = {'staking_limits': {'max_stake_fraction': 0.02,
                                     'max_weekly_stake_fraction': 0.10}}
        self.assertEqual(staking_limits(config), (0.02, 0.10))

    def test_partial_config_falls_back_per_key(self):
        config = {'staking_limits': {'max_stake_fraction': 0.025}}
        self.assertEqual(staking_limits(config), (0.025, 0.12))

    def test_nonsense_limits_rejected(self):
        for limits in ({'max_stake_fraction': 0},
                       {'max_stake_fraction': 1.5},
                       {'max_weekly_stake_fraction': -0.1}):
            with self.assertRaises(ValueError):
                staking_limits({'staking_limits': limits})

    def test_per_bet_cap_above_weekly_cap_rejected(self):
        with self.assertRaises(ValueError):
            staking_limits({'staking_limits': {
                'max_stake_fraction': 0.15,
                'max_weekly_stake_fraction': 0.12}})


class TestImplausibleEdgeFix(unittest.TestCase):
    """Item 2: edges at or above +20% EV are staked at Eighth-Kelly."""

    def test_forest_trips_the_threshold_and_is_sized_down(self):
        d = size_bet(bankroll=MW3_BANKROLL, **FOREST)

        self.assertAlmostEqual(d.expected_value, 0.354145, places=6)
        self.assertTrue(d.bet)
        self.assertEqual(d.confidence, LARGE_EDGE)
        self.assertEqual(d.multiplier, EIGHTH_KELLY)
        self.assertIn('implausible_edge', d.flags)
        # 989.11 x 0.125 x (0.354145 / 1.51)
        self.assertEqual(d.stake, 29.00)

    def test_forest_was_staked_at_quarter_kelly_before_the_fix(self):
        """The ledger's 57.99 is exactly what the defect produced."""
        pre_fix = size_bet(bankroll=MW3_BANKROLL, implausible_edge=1.0,
                           **FOREST)
        self.assertEqual(pre_fix.multiplier, QUARTER_KELLY)
        self.assertEqual(pre_fix.stake, 57.99)   # bet 00014 as recorded
        self.assertEqual(pre_fix.stake / 2, 28.995)

    def test_arsenal_is_below_the_threshold_and_untouched(self):
        d = size_bet(bankroll=MW3_BANKROLL, **ARSENAL)

        self.assertAlmostEqual(d.expected_value, 0.058278, places=6)
        self.assertTrue(d.bet)
        self.assertEqual(d.confidence, STANDARD)
        self.assertEqual(d.multiplier, QUARTER_KELLY)
        self.assertNotIn('implausible_edge', d.flags)
        self.assertEqual(d.stake, 20.89)         # bet 00015 as recorded

    def test_brentford_mw2_is_the_bug_reproduced(self):
        """
        Bet 00009. EV +20.12% cleared the threshold, the flag fired, and the
        stake was Quarter-Kelly anyway: 30.09 rather than 15.04. This is the
        regression the fix closes.
        """
        as_recorded = size_bet(bankroll=MW2_BANKROLL, implausible_edge=1.0,
                               **BRENTFORD_MW2)
        self.assertEqual(as_recorded.multiplier, QUARTER_KELLY)
        self.assertEqual(as_recorded.stake, 30.09)

        fixed = size_bet(bankroll=MW2_BANKROLL, **BRENTFORD_MW2)
        self.assertGreaterEqual(fixed.expected_value, 0.20)
        self.assertEqual(fixed.multiplier, EIGHTH_KELLY)
        self.assertEqual(fixed.stake, 15.04)


class TestPerBetCap(unittest.TestCase):
    """Item 1a: every stake clamped to the per-bet cap, every clamp logged."""

    def test_forest_pre_fix_stake_breaches_the_cap(self):
        """
        57.99 is 5.86% of 989.11 -- nearly double the 3% cap. Sized from the
        pre-fix multiplier so the cap is tested on its own, independently of
        the implausible-edge fix.
        """
        d = size_bet(bankroll=MW3_BANKROLL, implausible_edge=1.0, **FOREST)
        (d,), clamps = apply_stake_cap(
            [d], MW3_BANKROLL, max_fraction=0.03, logger=quiet)

        self.assertEqual(len(clamps), 1)
        self.assertEqual(d.stake, 29.67)          # 3% of 989.11
        self.assertTrue(d.capped)
        self.assertIn('per_bet_cap', d.flags)
        # The pre-cap figure survives, so the clamp is visible in the output.
        self.assertEqual(d.uncapped_stake, 57.99)
        self.assertAlmostEqual(d.stake, MW3_BANKROLL * d.stake_fraction,
                               places=2)

    def test_the_clamp_event_records_both_numbers(self):
        d = size_bet(bankroll=MW3_BANKROLL, implausible_edge=1.0, **FOREST)
        _, (event,) = apply_stake_cap(
            [d], MW3_BANKROLL, max_fraction=0.03, logger=quiet)

        self.assertEqual(event.fixture, "Nott'm Forest v Spurs")
        self.assertEqual(event.selection, 'home')
        self.assertEqual(event.calculated_stake, 57.99)
        self.assertEqual(event.capped_stake, 29.67)
        self.assertEqual(event.reduction, 28.32)
        self.assertEqual(event.cap_fraction, 0.03)
        self.assertIn('57.99', event.describe())
        self.assertIn('29.67', event.describe())

    def test_the_clamp_is_logged(self):
        logged = []
        d = size_bet(bankroll=MW3_BANKROLL, implausible_edge=1.0, **FOREST)
        apply_stake_cap([d], MW3_BANKROLL, max_fraction=0.03,
                        logger=logged.append)

        self.assertEqual(len(logged), 1)
        self.assertIn('CAP', logged[0])
        self.assertIn("Nott'm Forest v Spurs", logged[0])

    def test_forest_after_the_fix_sits_just_under_the_cap(self):
        """
        29.00 against a 29.67 cap. The two fixes are independent and both are
        needed: the flag fix does most of the work here, and the cap is what
        guarantees it regardless.
        """
        d = size_bet(bankroll=MW3_BANKROLL, **FOREST)
        (d,), clamps = apply_stake_cap(
            [d], MW3_BANKROLL, max_fraction=0.03, logger=quiet)

        self.assertEqual(clamps, [])
        self.assertEqual(d.stake, 29.00)
        self.assertFalse(d.capped)

    def test_arsenal_is_under_the_cap_and_not_clamped(self):
        """20.89 is 2.11% of bankroll -- inside 3%, so nothing happens."""
        d = size_bet(bankroll=MW3_BANKROLL, **ARSENAL)
        (d,), clamps = apply_stake_cap(
            [d], MW3_BANKROLL, max_fraction=0.03, logger=quiet)

        self.assertEqual(clamps, [])
        self.assertEqual(d.stake, 20.89)
        self.assertFalse(d.capped)
        self.assertNotIn('per_bet_cap', d.flags)
        # Reported either way, so the report never has to infer it.
        self.assertEqual(d.uncapped_stake, 20.89)

    def test_no_stake_ever_exceeds_the_cap(self):
        decisions = [size_bet(bankroll=MW3_BANKROLL, **FOREST),
                     size_bet(bankroll=MW3_BANKROLL, **ARSENAL)]
        decisions, _ = apply_stake_cap(
            decisions, MW3_BANKROLL, max_fraction=0.03, logger=quiet)
        cap = MW3_BANKROLL * 0.03
        for d in decisions:
            self.assertLessEqual(d.stake, cap + 0.01)

    def test_the_cap_only_ever_reduces(self):
        decisions = [size_bet(bankroll=MW3_BANKROLL, **FOREST),
                     size_bet(bankroll=MW3_BANKROLL, **ARSENAL)]
        before = [d.stake for d in decisions]
        decisions, _ = apply_stake_cap(
            decisions, MW3_BANKROLL, max_fraction=0.03, logger=quiet)
        for d, was in zip(decisions, before):
            self.assertLessEqual(d.stake, was)

    def test_a_blocked_bet_is_not_clamped(self):
        d = size_bet(bankroll=MW3_BANKROLL, implausible_edge=1.0, **FOREST)
        d.bet = False
        d.stake = 0.0
        _, clamps = apply_stake_cap([d], MW3_BANKROLL, max_fraction=0.03,
                                    logger=quiet)
        self.assertEqual(clamps, [])


class TestWeeklyCap(unittest.TestCase):
    """Item 1b: the weekly total is flagged, never silently rescaled."""

    def _mw2_book(self, implausible_edge=1.0):
        return [size_bet(fixture=fixture, market='1x2', selection=selection,
                         probability=p, odds=odds, bankroll=MW2_BANKROLL,
                         implausible_edge=implausible_edge)
                for fixture, selection, p, odds in MW2_BOOK]

    def test_mw2_as_placed_breached_the_weekly_cap(self):
        """
        The six MW2 bets totalled 153.76 -- 15.12% of a 1016.75 bankroll,
        against a 12% (122.01) cap. Nothing in the system said so at the time.
        """
        decisions = self._mw2_book()
        exposure, finding = check_weekly_exposure(
            decisions, MW2_BANKROLL, max_fraction=0.12)

        self.assertEqual(exposure['total_stake'], 153.76)
        self.assertEqual(exposure['exposure_pct'], 15.12)
        self.assertEqual(exposure['weekly_cap_amount'], 122.01)
        self.assertEqual(exposure['over_cap_by'], 31.75)
        self.assertTrue(exposure['breached'])
        self.assertIsNotNone(finding)
        self.assertEqual(finding.kind, 'weekly_exposure_exceeded')

    def test_a_breach_does_not_rescale_anything(self):
        """The operator decides what to drop. The system only reports."""
        decisions = self._mw2_book()
        before = [d.stake for d in decisions]
        exposure, finding = check_weekly_exposure(
            decisions, MW2_BANKROLL, max_fraction=0.12)

        self.assertTrue(exposure['breached'])
        self.assertEqual([d.stake for d in decisions], before)
        self.assertEqual(sum(before), 153.76)

    def test_a_breach_does_not_block(self):
        decisions = self._mw2_book()
        _, finding = check_weekly_exposure(decisions, MW2_BANKROLL,
                                           max_fraction=0.12)
        self.assertFalse(finding.blocks)
        self.assertTrue(all(d.bet for d in decisions))

    def test_the_breach_says_stakes_were_not_rescaled(self):
        decisions = self._mw2_book()
        exposure, finding = check_weekly_exposure(
            decisions, MW2_BANKROLL, max_fraction=0.12)
        self.assertIn('NOT been rescaled', finding.detail)
        self.assertIn('NOT rescaled', exposure['action_taken'])

    def test_mw2_with_both_fixes_applied_comes_in_under_the_cap(self):
        """
        Same six selections, sized under the fixed rule and the per-bet cap:
        90.20, or 8.87%. The breach was a consequence of the flag defect, not
        of the selections themselves.
        """
        decisions = self._mw2_book(implausible_edge=0.20)
        decisions, _ = apply_stake_cap(decisions, MW2_BANKROLL,
                                       max_fraction=0.03, logger=quiet)
        exposure, finding = check_weekly_exposure(
            decisions, MW2_BANKROLL, max_fraction=0.12)

        self.assertFalse(exposure['breached'])
        self.assertIsNone(finding)
        self.assertLess(exposure['total_stake'],
                        exposure['weekly_cap_amount'])

    def test_this_week_is_within_the_weekly_cap(self):
        """Forest + Arsenal: 49.89 of 989.11 = 5.04%, well inside 12%."""
        decisions = [size_bet(bankroll=MW3_BANKROLL, **FOREST),
                     size_bet(bankroll=MW3_BANKROLL, **ARSENAL)]
        decisions, _ = apply_stake_cap(decisions, MW3_BANKROLL,
                                       max_fraction=0.03, logger=quiet)
        exposure, finding = check_weekly_exposure(
            decisions, MW3_BANKROLL, max_fraction=0.12)

        self.assertEqual(exposure['total_stake'], 49.89)
        self.assertEqual(exposure['exposure_pct'], 5.04)
        self.assertFalse(exposure['breached'])
        self.assertIsNone(finding)

    def test_only_live_bets_count_toward_the_week(self):
        forest = size_bet(bankroll=MW3_BANKROLL, **FOREST)
        arsenal = size_bet(bankroll=MW3_BANKROLL, **ARSENAL)
        arsenal.bet = False
        arsenal.stake = 0.0

        exposure, _ = check_weekly_exposure([forest, arsenal], MW3_BANKROLL)
        self.assertEqual(exposure['bets'], 1)
        self.assertEqual(exposure['total_stake'], forest.stake)

    def test_exposure_is_reported_even_when_within_cap(self):
        """The number is always in front of the operator, breach or not."""
        decisions = [size_bet(bankroll=MW3_BANKROLL, **ARSENAL)]
        exposure, finding = check_weekly_exposure(decisions, MW3_BANKROLL)
        self.assertIsNone(finding)
        self.assertFalse(exposure['breached'])
        self.assertEqual(exposure['exposure_pct'], 2.11)


if __name__ == '__main__':
    unittest.main(verbosity=2)
