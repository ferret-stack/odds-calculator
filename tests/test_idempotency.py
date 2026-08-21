"""
The idempotency guard: a fixture/selection already on the book is never
written twice.

`Ledger.settle()` and the staking arithmetic are covered elsewhere. What is
new here is bet IDENTITY -- what makes two bets the same bet, what the
pipeline does when it finds one, and what --force changes. The accounting
consequence of a forced re-price is covered too: a superseded bet stays in
the file but leaves the record, so one position is never counted as two.
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.bankroll import LOST, PENDING, VOID, WON, Ledger
from pipeline.run_pipeline import place_bets, run
from pipeline.staking import size_bet


def write(path, payload):
    Path(path).write_text(json.dumps(payload))


class LedgerTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'bankroll.json'
        self.ledger = Ledger(path=self.path, starting_bankroll=1000.0)

    def tearDown(self):
        self.tmp.cleanup()

    def _decision(self, fixture='Arsenal v Everton', market='1x2',
                  selection='home', match_date='2026-09-20', odds=2.10,
                  p=0.55):
        return size_bet(fixture, market, selection, p, odds,
                        self.ledger.staking_bankroll, match_date=match_date)

    def _place(self, **kwargs):
        return self.ledger.place(self._decision(**kwargs))


class TestFindDuplicate(LedgerTestCase):
    def test_empty_ledger_has_no_duplicates(self):
        self.assertIsNone(self.ledger.find_duplicate(
            'Arsenal v Everton', '1x2', 'home', '2026-09-20'))

    def test_exact_key_is_a_duplicate(self):
        bet = self._place()
        found = self.ledger.find_duplicate(
            'Arsenal v Everton', '1x2', 'home', '2026-09-20')
        self.assertIs(found, bet)

    def test_same_fixture_on_a_different_date_is_a_different_match(self):
        """The reverse fixture next season is not the bet already struck."""
        self._place(match_date='2026-09-20')
        self.assertIsNone(self.ledger.find_duplicate(
            'Arsenal v Everton', '1x2', 'home', '2027-02-14'))

    def test_different_selection_is_not_a_duplicate(self):
        self._place(selection='home')
        self.assertIsNone(self.ledger.find_duplicate(
            'Arsenal v Everton', '1x2', 'away', '2026-09-20'))

    def test_different_market_is_not_a_duplicate(self):
        self._place(market='1x2')
        self.assertIsNone(self.ledger.find_duplicate(
            'Arsenal v Everton', 'btts', 'home', '2026-09-20'))

    def test_fixture_name_matched_case_and_whitespace_insensitively(self):
        bet = self._place(fixture='Arsenal v Everton')
        found = self.ledger.find_duplicate(
            '  arsenal   V  EVERTON ', '1x2', 'home', '2026-09-20')
        self.assertIs(found, bet)

    def test_a_settled_bet_is_still_a_duplicate(self):
        """Grading a bet does not take it off the book."""
        bet = self._place()
        self.ledger.settle(bet.bet_id, WON)
        found = self.ledger.find_duplicate(
            'Arsenal v Everton', '1x2', 'home', '2026-09-20')
        self.assertIs(found, bet)
        self.assertEqual(found.status, WON)

    def test_bet_with_no_match_date_matches_on_the_other_three_keys(self):
        """
        Records written before match dates existed carry ''. An unknown date
        cannot be shown to be a different match, so the guard refuses.
        """
        bet = self._place(match_date='')
        found = self.ledger.find_duplicate(
            'Arsenal v Everton', '1x2', 'home', '2026-09-20')
        self.assertIs(found, bet)


class PipelineTestCase(unittest.TestCase):
    """A synthetic data dir whose single fixture produces exactly one +EV bet."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data = Path(self.tmp.name)
        (self.data / 'qualitative').mkdir()

        write(self.data / 'current_elo.json', {
            'Arsenal': {'elo': 2009, 'rank': 1},
            'Everton': {'elo': 1784, 'rank': 13},
        })
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
        write(self.data / 'matches_data.json', [])
        write(self.data / 'cup_fixtures.json', {'teams': {}})
        write(self.data / 'pipeline_config.json',
              {'starting_bankroll': 1000.0, 'low_confidence_bands': [9, 10]})
        self._fixtures()

    def tearDown(self):
        self.tmp.cleanup()

    def _fixtures(self, entries=None):
        write(self.data / 'upcoming_fixtures.json', entries or [{
            'home_team': 'Arsenal', 'away_team': 'Everton',
            'date': '2026-09-20', 'time': '15:00', 'game_id': 'test1',
            'bookmaker_odds': {'home': 1.90, 'draw': 4.00, 'away': 6.00},
        }])

    def _run(self, **kwargs):
        return run(data_dir=self.data, **kwargs)

    def _ledger(self):
        return Ledger(path=self.data / 'bankroll.json')


class TestCleanRun(PipelineTestCase):
    def test_a_first_run_places_every_positive_ev_bet(self):
        report = self._run()
        placed = [s for s in report['selections'] if s['bet']]
        self.assertTrue(placed, 'fixture should produce at least one bet')
        self.assertEqual(report['totals']['bets_placed'], len(placed))
        self.assertEqual(len(self._ledger().bets), len(placed))

    def test_a_clean_run_reports_no_duplicates(self):
        dup = self._run()['duplicate_check']
        self.assertEqual(dup['skipped'], [])
        self.assertEqual(dup['forced'], [])
        self.assertFalse(dup['force'])


class TestDuplicateRun(PipelineTestCase):
    def test_a_second_run_over_the_same_fixtures_places_nothing(self):
        self._run()
        report = self._run()
        self.assertEqual(report['totals']['bets_placed'], 0)

    def test_the_ledger_is_unchanged_by_the_second_run(self):
        self._run()
        before = [b.bet_id for b in self._ledger().bets]
        self._run()
        self.assertEqual([b.bet_id for b in self._ledger().bets], before)

    def test_the_skip_names_the_colliding_bet_and_its_reason(self):
        self._run()
        skipped = self._run()['duplicate_check']['skipped']
        self.assertTrue(skipped)
        for s in skipped:
            self.assertEqual(s['existing_status'], PENDING)
            self.assertRegex(s['existing_bet_id'], r'^\d{5}$')
            self.assertIn('already on the book', s['reason'])
            self.assertIn('--force', s['reason'])

    def test_a_fixture_listed_twice_is_blocked_before_it_reaches_the_book(self):
        """
        The same fixture twice in one feed is caught upstream, by the
        same-market sanity check, and never reaches the ledger at all.
        """
        fixture = {
            'home_team': 'Arsenal', 'away_team': 'Everton',
            'date': '2026-09-20', 'time': '15:00', 'game_id': 'test1',
            'bookmaker_odds': {'home': 1.90, 'draw': 4.00, 'away': 6.00},
        }
        self._fixtures([fixture, dict(fixture)])
        report = self._run()

        self.assertEqual(report['totals']['bets_placed'], 0)
        blocking = [f for f in report['sanity_findings'] if f['blocking']]
        self.assertTrue(blocking)
        self.assertEqual(blocking[0]['kind'], 'same_market_multiple_ev')


class TestIntraRunGuard(LedgerTestCase):
    """
    place_bets' own last line of defence.

    Duplicate fixtures are normally stopped by the same-market sanity check
    before they get here, so this is belt-and-braces -- but it is what stops
    a duplicate slipping through on a market the sanity check does not pair,
    and --force must not turn one into a supersede chain.
    """

    def _decisions(self):
        d = self._decision()
        return [d, self._decision()]

    def test_two_identical_decisions_in_one_run_place_once(self):
        placed, skipped, forced = place_bets(self.ledger, self._decisions())
        self.assertEqual(len(placed), 1)
        self.assertEqual(len(skipped), 1)
        self.assertIn('within this same run', skipped[0]['reason'])
        self.assertEqual(len(self.ledger.bets), 1)

    def test_force_does_not_turn_an_intra_run_repeat_into_a_supersede(self):
        placed, skipped, forced = place_bets(
            self.ledger, self._decisions(), force=True)
        self.assertEqual(forced, [])
        self.assertEqual(len(skipped), 1)
        self.assertEqual(len(self.ledger.bets), 1)
        self.assertIsNone(self.ledger.bets[0].supersedes)


class TestForce(PipelineTestCase):
    def test_force_re_prices_a_pending_duplicate(self):
        first = self._run()
        report = self._run(force=True)
        self.assertEqual(report['totals']['bets_forced'],
                         first['totals']['bets_placed'])
        self.assertEqual(report['totals']['bets_skipped_as_duplicates'], 0)
        self.assertEqual(len(self._ledger().bets),
                         first['totals']['bets_placed'] * 2)

    def test_the_supersede_links_point_at_each_other(self):
        self._run()
        self._run(force=True)
        ledger = self._ledger()
        new = [b for b in ledger.bets if b.supersedes is not None]
        self.assertTrue(new)
        for bet in new:
            old = next(b for b in ledger.bets if b.bet_id == bet.supersedes)
            self.assertEqual(old.superseded_by, bet.bet_id)
            self.assertIn('forced re-price', bet.notes)
            self.assertIn(old.bet_id, bet.notes)

    def test_force_refuses_a_duplicate_that_has_already_settled(self):
        self._run()
        ledger = self._ledger()
        for bet in ledger.bets:
            ledger.settle(bet.bet_id, LOST)
        ledger.save()

        report = self._run(force=True)
        self.assertEqual(report['totals']['bets_forced'], 0)
        skipped = report['duplicate_check']['skipped']
        self.assertTrue(skipped)
        for s in skipped:
            self.assertIn('--force does not apply', s['reason'])
        self.assertTrue(all(b.superseded_by is None
                            for b in self._ledger().bets))

    def test_the_report_records_that_force_was_used(self):
        self._run()
        report = self._run(force=True)
        self.assertTrue(report['duplicate_check']['force'])
        for f in report['duplicate_check']['forced']:
            self.assertEqual(f['existing_status'], PENDING)
            self.assertIn('bet_id', f)

    def test_dry_run_with_force_writes_nothing(self):
        self._run()
        before = (self.data / 'bankroll.json').read_text()
        report = self._run(dry_run=True, force=True)
        self.assertTrue(report['duplicate_check']['forced'])
        self.assertEqual((self.data / 'bankroll.json').read_text(), before)


class TestSupersededExcluded(LedgerTestCase):
    """A forced re-price is one position, not two."""

    def _pair(self):
        """An original bet and the forced re-price that supersedes it."""
        old = self._place(odds=2.10)
        new = self.ledger.place(self._decision(odds=2.50),
                                supersedes=old.bet_id)
        return old, new

    def test_superseded_bet_is_excluded_from_wins_losses_and_strike_rate(self):
        old, new = self._pair()
        self.ledger.settle(old.bet_id, LOST)
        self.ledger.settle(new.bet_id, WON)

        summary = self.ledger.summary()
        self.assertEqual(summary['wins'], 1)
        self.assertEqual(summary['losses'], 0)
        self.assertEqual(summary['strike_rate'], 1.0)
        self.assertEqual(summary['bets_settled'], 1)

    def test_superseded_stake_is_out_of_total_staked_and_the_roi_divisor(self):
        old, new = self._pair()
        self.ledger.settle(old.bet_id, LOST)
        self.ledger.settle(new.bet_id, WON)

        summary = self.ledger.summary()
        self.assertEqual(summary['total_staked'], round(new.stake, 2))
        self.assertAlmostEqual(
            summary['roi'], round(new.profit / new.stake, 4), places=4)

    def test_superseded_profit_is_out_of_realised_pnl_and_bankroll(self):
        old, new = self._pair()
        self.ledger.settle(old.bet_id, LOST)
        self.ledger.settle(new.bet_id, WON)

        self.assertEqual(self.ledger.realised_pnl, new.profit)
        self.assertEqual(self.ledger.bankroll, round(1000.0 + new.profit, 2))

    def test_a_pending_superseded_bet_is_not_committed_capital(self):
        old, new = self._pair()
        self.assertEqual(old.status, PENDING)
        self.assertEqual(self.ledger.committed, new.stake)
        self.assertEqual(self.ledger.staking_bankroll,
                         round(1000.0 - new.stake, 2))

    def test_bets_total_still_counts_it_and_bets_superseded_reports_it(self):
        self._pair()
        summary = self.ledger.summary()
        self.assertEqual(summary['bets_total'], 2)
        self.assertEqual(summary['bets_superseded'], 1)
        self.assertEqual(summary['bets_open'], 1)
        self.assertIn('superseded', self.ledger.format_summary())

    def test_the_links_survive_a_save_and_reload(self):
        old, new = self._pair()
        self.ledger.save()

        reloaded = Ledger(path=self.path)
        self.assertEqual(len(reloaded.bets), 2)
        self.assertEqual(len(reloaded.counted_bets), 1)
        by_id = {b.bet_id: b for b in reloaded.bets}
        self.assertEqual(by_id[old.bet_id].superseded_by, new.bet_id)
        self.assertEqual(by_id[new.bet_id].supersedes, old.bet_id)

    def test_forcing_twice_chains_and_counts_only_the_last(self):
        old, middle = self._pair()
        last = self.ledger.place(self._decision(odds=3.00),
                                 supersedes=middle.bet_id)

        self.assertEqual(self.ledger.summary()['bets_superseded'], 2)
        self.assertEqual([b.bet_id for b in self.ledger.counted_bets],
                         [last.bet_id])
        # The live position is what a repeat run collides with, not the
        # records it replaced.
        self.assertIs(self.ledger.find_duplicate(
            'Arsenal v Everton', '1x2', 'home', '2026-09-20'), last)


if __name__ == '__main__':
    unittest.main(verbosity=2)
