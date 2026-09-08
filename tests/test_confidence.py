"""
Sample size and confidence intervals on band-derived probabilities.

Instrumentation only: these tests also pin down that nothing here influences
a stake.
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from elo_calculator import calculate_fair_odds
from pipeline.confidence import (
    Z_95, band_evidence, find_band, summarise, wilson_interval,
)

DATA = Path(__file__).resolve().parents[1] / 'data'
ELO_BANDS = json.loads((DATA / 'elo_bands.json').read_text())


class TestWilsonInterval(unittest.TestCase):
    def test_known_value(self):
        """
        p-hat = 0.5 on n = 100, z = 1.96.
          centre = (0.5 + 1.9208/200) / (1 + 0.038416) = 0.5
          margin = (1.96/1.038416) * sqrt(0.0025 + 0.000092237)
        Published Wilson bounds for 50/100 are (0.4038, 0.5962).
        """
        low, high = wilson_interval(50, 100)
        self.assertAlmostEqual(low, 0.4038, places=4)
        self.assertAlmostEqual(high, 0.5962, places=4)

    def test_interval_brackets_the_point_estimate(self):
        for successes, n in ((1, 4), (13, 26), (43, 51), (275, 674)):
            low, high = wilson_interval(successes, n)
            self.assertLessEqual(low, successes / n)
            self.assertLessEqual(successes / n, high)

    def test_boundary_proportions_do_not_collapse(self):
        """
        The reason for Wilson over Wald. Band 10 is 4/4 and Band 9 is 0/6;
        Wald would report zero width for both, which is precisely backwards
        for the thinnest rows in the file.
        """
        low, high = wilson_interval(4, 4)
        self.assertGreater(high - low, 0.4)
        self.assertLess(low, 1.0)
        self.assertEqual(high, 1.0)

        low, high = wilson_interval(0, 6)
        self.assertGreater(high - low, 0.3)
        self.assertEqual(low, 0.0)
        self.assertGreater(high, 0.0)

    def test_interval_stays_inside_zero_one(self):
        for n in (1, 4, 6, 26, 674):
            for successes in range(n + 1):
                low, high = wilson_interval(successes, n)
                self.assertGreaterEqual(low, 0.0)
                self.assertLessEqual(high, 1.0)

    def test_interval_narrows_as_the_sample_grows(self):
        """Same proportion, more matches: a tighter interval, every time."""
        widths = [wilson_interval(n // 2, n)[1] - wilson_interval(n // 2, n)[0]
                  for n in (4, 26, 100, 674)]
        self.assertEqual(widths, sorted(widths, reverse=True))

    def test_empty_sample_is_the_whole_range(self):
        self.assertEqual(wilson_interval(0, 0), (0.0, 1.0))

    def test_invalid_inputs_rejected(self):
        with self.assertRaises(ValueError):
            wilson_interval(5, 4)
        with self.assertRaises(ValueError):
            wilson_interval(-1, 4)
        with self.assertRaises(ValueError):
            wilson_interval(0, -1)


class TestBandEvidence(unittest.TestCase):
    """Against the real band file and this week's real fixtures."""

    def setUp(self):
        elo = json.loads((DATA / 'current_elo.json').read_text())
        self.elo = {t: (v['elo'] if isinstance(v, dict) else v)
                    for t, v in elo.items()}

    def _evidence(self, home, away):
        model = calculate_fair_odds(self.elo[home], self.elo[away], ELO_BANDS)
        return model, band_evidence(model, ELO_BANDS)

    def test_forest_carries_its_band_and_sample_size(self):
        """Nott'm Forest v Spurs: 61 ELO points apart -> Band 2, n = 528."""
        model, evidence = self._evidence("Nott'm Forest", 'Spurs')
        home = evidence['home_win']

        self.assertEqual(model['meta']['band'], 2)
        self.assertEqual(home['band'], 2)
        self.assertEqual(home['band_range'], '51-100')
        self.assertEqual(home['match_count'], 528)
        self.assertEqual(home['band_proportion'], 0.4924)
        self.assertEqual(home['interval'], 'wilson_95')

    def test_arsenal_carries_its_band_and_sample_size(self):
        """Arsenal v Chelsea: 185 points apart -> Band 4, n = 285."""
        model, evidence = self._evidence('Arsenal', 'Chelsea')
        home = evidence['home_win']

        self.assertEqual(model['meta']['band'], 4)
        self.assertEqual(home['match_count'], 285)
        self.assertEqual(home['band_proportion'], 0.5825)

    def test_the_adjusted_interval_brackets_the_staked_probability(self):
        """
        The number in the output table has to sit inside the interval printed
        beside it, or the column is worse than useless.
        """
        for home, away in (("Nott'm Forest", 'Spurs'), ('Arsenal', 'Chelsea')):
            model, evidence = self._evidence(home, away)
            for key in ('home_win', 'draw', 'away_win'):
                entry = evidence[key]
                probability = model[key]['probability']
                self.assertLessEqual(entry['adjusted_ci_low'], probability,
                                     f'{home} v {away} / {key}')
                self.assertLessEqual(probability, entry['adjusted_ci_high'],
                                     f'{home} v {away} / {key}')
                self.assertEqual(entry['adjusted_probability'], probability)

    def test_every_selection_carries_evidence(self):
        _, evidence = self._evidence('Arsenal', 'Chelsea')
        self.assertEqual(set(evidence), {'home_win', 'draw', 'away_win'})
        for entry in evidence.values():
            self.assertGreater(entry['match_count'], 0)
            self.assertIn('ci_low', entry)
            self.assertIn('ci_high', entry)

    def test_thin_bands_report_wide_intervals(self):
        """
        Band 10 rests on 4 matches. Its interval must be visibly wider than
        Band 1's, which rests on 674 -- that contrast is the entire point of
        the column.
        """
        def width(band_number):
            band = find_band(band_number, ELO_BANDS)
            n = band['total_games']
            successes = round(band['stronger_win_pct'] * n)
            low, high = wilson_interval(successes, n)
            return high - low

        self.assertGreater(width(10), width(1) * 5)
        self.assertGreater(width(9), width(2))

    def test_orientation_follows_the_stronger_team(self):
        """
        With the stronger side away, home_win must come from the WEAKER-team
        column, not the stronger one.
        """
        weak, strong = 1500, 1700
        model = calculate_fair_odds(weak, strong, ELO_BANDS)
        evidence = band_evidence(model, ELO_BANDS)
        band = find_band(model['meta']['band'], ELO_BANDS)

        self.assertEqual(model['meta']['stronger_team'], 'away')
        self.assertEqual(evidence['home_win']['band_proportion'],
                         round(band['weaker_win_pct'], 4))
        self.assertEqual(evidence['away_win']['band_proportion'],
                         round(band['stronger_win_pct'], 4))

    def test_zero_rate_band_does_not_break(self):
        """
        Bands 9 and 10 have rates of exactly 0.0 (Band 10: both draw and
        weaker-win), so the raw -> adjusted carry-through must not divide by
        a rate. It does not: the factor is derived from the venue multipliers
        and the normalisation total, neither of which involves the rate being
        scaled.

        The model dict is built directly rather than via calculate_fair_odds
        because that function raises ZeroDivisionError on Band 10 -- it takes
        1/probability for fair odds, and Band 10's draw rate is 0.0. That is a
        pre-existing defect in the ELO pricing code, out of scope here, and
        this test must not depend on it.
        """
        for band_number in (9, 10):
            model = {'meta': {'band': band_number, 'stronger_team': 'home'},
                     'home_win': {'probability': 1.0},
                     'draw': {'probability': 0.0},
                     'away_win': {'probability': 0.0}}
            evidence = band_evidence(model, ELO_BANDS)
            self.assertEqual(set(evidence),
                             {'home_win', 'draw', 'away_win'}, band_number)
            for entry in evidence.values():
                self.assertGreaterEqual(entry['adjusted_ci_low'], 0.0)
                self.assertLessEqual(entry['adjusted_ci_high'], 1.0)
            # A 0/6 rate still gets a non-zero upper bound: "we have not seen
            # one" is not "it cannot happen".
            self.assertGreater(evidence['away_win']['adjusted_ci_high'], 0.0)

    def test_missing_band_returns_empty_rather_than_raising(self):
        """Instrumentation must never be what stops a run."""
        model = {'meta': {'band': 99, 'stronger_team': 'home'}}
        self.assertEqual(band_evidence(model, ELO_BANDS), {})

    def test_summarise_names_the_sample_size(self):
        _, evidence = self._evidence('Arsenal', 'Chelsea')
        line = summarise(evidence['home_win'])
        self.assertIn('n=285', line)
        self.assertIn('band 4', line)


class TestInstrumentationOnly(unittest.TestCase):
    def test_evidence_does_not_change_the_stake(self):
        """
        The same selection sized with and without band evidence attached must
        produce an identical stake. This is the boundary the ticket draws:
        the interval is reported, and it is not yet an input.
        """
        from pipeline.staking import size_bet

        plain = size_bet('Arsenal v Chelsea', '1x2', 'home', 0.6262, 1.69,
                         989.11)
        with_evidence = size_bet(
            'Arsenal v Chelsea', '1x2', 'home', 0.6262, 1.69, 989.11,
            band_evidence={'match_count': 4, 'ci_low': 0.0, 'ci_high': 1.0})

        self.assertEqual(plain.stake, with_evidence.stake)
        self.assertEqual(plain.multiplier, with_evidence.multiplier)
        self.assertEqual(plain.confidence, with_evidence.confidence)
        self.assertEqual(with_evidence.band_evidence['match_count'], 4)


if __name__ == '__main__':
    unittest.main(verbosity=2)
