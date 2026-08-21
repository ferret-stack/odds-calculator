"""
Tests for the settlement trigger path.

Ledger.settle() is already covered in test_bankroll.py. What is new here is
everything that decides WHICH bets get settled and AS WHAT: grading a
selection against a scoreline, reading results the operator typed up, pulling
them out of the scraped match store, and the CLI that drives the pass and
writes bankroll.json.

The behaviour these lean on hardest is the refusal to guess: a bet with no
result, or a selection with no grading rule, must come back pending and named
rather than quietly graded as a loss.
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.bankroll import LOST, PENDING, VOID, WON, Ledger
from pipeline.settle_results import (
    Unresolved, grade_selection, index_matches, load_results, main,
    match_for_bet, parse_override, parse_score, resolve, run, settle_pending,
)
from pipeline.staking import size_bet


class TestGrading(unittest.TestCase):
    def test_1x2(self):
        self.assertEqual(grade_selection('home', 2, 1), WON)
        self.assertEqual(grade_selection('home', 1, 1), LOST)
        self.assertEqual(grade_selection('away', 1, 2), WON)
        self.assertEqual(grade_selection('away', 2, 2), LOST)
        self.assertEqual(grade_selection('draw', 1, 1), WON)
        self.assertEqual(grade_selection('draw', 2, 1), LOST)

    def test_goal_lines(self):
        self.assertEqual(grade_selection('over_25', 2, 1), WON)
        self.assertEqual(grade_selection('over_25', 1, 1), LOST)
        self.assertEqual(grade_selection('under_25', 1, 1), WON)
        self.assertEqual(grade_selection('under_25', 2, 1), LOST)
        self.assertEqual(grade_selection('over_05', 0, 0), LOST)
        self.assertEqual(grade_selection('under_05', 0, 0), WON)
        self.assertEqual(grade_selection('over_45', 3, 2), WON)
        self.assertEqual(grade_selection('under_35', 2, 1), WON)

    def test_over_and_under_the_same_line_never_agree(self):
        for home in range(5):
            for away in range(5):
                for line in ('05', '15', '25', '35', '45'):
                    over = grade_selection(f'over_{line}', home, away)
                    under = grade_selection(f'under_{line}', home, away)
                    self.assertNotEqual(
                        over, under,
                        f'over_{line} and under_{line} both {over} '
                        f'on {home}-{away}')

    def test_btts(self):
        self.assertEqual(grade_selection('btts_yes', 1, 1), WON)
        self.assertEqual(grade_selection('btts_yes', 3, 0), LOST)
        self.assertEqual(grade_selection('btts_no', 3, 0), WON)
        self.assertEqual(grade_selection('btts_no', 1, 2), LOST)

    def test_unknown_selection_raises_rather_than_grading(self):
        with self.assertRaises(ValueError):
            grade_selection('correct_score_2_1', 2, 1)

    def test_negative_scoreline_rejected(self):
        with self.assertRaises(ValueError):
            grade_selection('home', -1, 0)


class TestResultsFile(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'results.json'

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, payload):
        self.path.write_text(json.dumps(payload))
        return self.path

    def test_missing_file_is_empty_not_an_error(self):
        self.assertEqual(load_results(self.path), {})

    def test_bare_list_and_wrapped_object_both_read(self):
        entry = {'fixture': 'A v B', 'score': '2-1'}
        self.assertEqual(list(load_results(self.write([entry]))), ['a v b'])
        self.assertEqual(list(load_results(self.write({'results': [entry]}))),
                         ['a v b'])

    def test_team_fields_accepted_instead_of_fixture_name(self):
        results = load_results(self.write(
            [{'home_team': 'A', 'away_team': 'B', 'score': '2-1'}]))
        self.assertIn('a v b', results)

    def test_fixture_lookup_ignores_case_and_spacing(self):
        results = load_results(self.write(
            [{'fixture': '  Nott\'m  Forest   v Bournemouth ', 'score': '1-0'}]))
        self.assertIn("nott'm forest v bournemouth", results)

    def test_duplicate_fixture_rejected(self):
        with self.assertRaises(ValueError):
            load_results(self.write([{'fixture': 'A v B', 'score': '2-1'},
                                     {'fixture': 'A v B', 'score': '0-0'}]))

    def test_entry_without_a_fixture_rejected(self):
        with self.assertRaises(ValueError):
            load_results(self.write([{'score': '2-1'}]))

    def test_score_forms(self):
        self.assertEqual(parse_score({'score': '2-1'}), (2, 1))
        self.assertEqual(parse_score({'score': '2 - 1'}), (2, 1))
        self.assertEqual(parse_score({'score': '2:1'}), (2, 1))
        self.assertEqual(parse_score({'home_goals': 0, 'away_goals': 3}), (0, 3))

    def test_void_forms_read_as_no_score(self):
        self.assertIsNone(parse_score({'status': 'void'}))
        self.assertIsNone(parse_score({'void': True}))

    def test_unreadable_score_rejected(self):
        with self.assertRaises(ValueError):
            parse_score({'score': 'two one'})

    def test_entry_with_neither_score_nor_void_rejected(self):
        with self.assertRaises(ValueError):
            parse_score({'fixture': 'A v B'})


class SettlementTestCase(unittest.TestCase):
    """A ledger with three open bets on three different fixtures."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data = Path(self.tmp.name)
        self.ledger = Ledger(path=self.data / 'bankroll.json',
                             starting_bankroll=1000.0)
        self.home = self._place('Arsenal v Everton', 'home', 0.60, 2.10)
        self.draw = self._place('Spurs v Leeds', 'draw', 0.40, 3.60)
        self.away = self._place('Burnley v Wolves', 'away', 0.45, 2.80)
        self.ledger.save()

    def tearDown(self):
        self.tmp.cleanup()

    def _place(self, fixture, selection, probability, odds,
               placed_at='2026-05-20 12:00:00'):
        # Fixed placement date: the scraped-store lookup is relative to it,
        # so a wall-clock default would make these tests depend on the day
        # they run.
        decision = size_bet(fixture, '1x2', selection, probability, odds,
                            self.ledger.staking_bankroll)
        self.assertTrue(decision.bet, decision.reason)
        return self.ledger.place(decision, placed_at=placed_at)

    def write_results(self, entries, name='results.json'):
        path = self.data / name
        path.write_text(json.dumps({'results': entries}))
        return path


class TestSettlePending(SettlementTestCase):
    def test_pending_bets_move_to_won_and_lost(self):
        results = load_results(self.write_results([
            {'fixture': 'Arsenal v Everton', 'score': '2-0'},
            {'fixture': 'Spurs v Leeds', 'score': '1-0'},
        ]))
        settled, unresolved = settle_pending(self.ledger, results)

        self.assertEqual([(b.bet_id, s) for b, s in settled],
                         [(self.home.bet_id, WON), (self.draw.bet_id, LOST)])
        self.assertEqual(self.home.status, WON)
        self.assertEqual(self.draw.status, LOST)
        self.assertEqual([b.bet_id for b, _ in unresolved],
                         [self.away.bet_id])
        self.assertEqual(self.away.status, PENDING)

    def test_settled_bet_records_the_scoreline_it_was_graded_on(self):
        results = load_results(self.write_results(
            [{'fixture': 'Arsenal v Everton', 'score': '2-0'}]))
        settle_pending(self.ledger, results)
        self.assertIn('2-0', self.home.notes)
        self.assertIsNotNone(self.home.settled_at)

    def test_void_result_settles_void_and_is_pnl_neutral(self):
        results = load_results(self.write_results([
            {'fixture': 'Arsenal v Everton', 'status': 'void',
             'notes': 'match abandoned'},
        ]))
        settle_pending(self.ledger, results)
        self.assertEqual(self.home.status, VOID)
        self.assertEqual(self.home.profit, 0.0)
        self.assertEqual(self.ledger.realised_pnl, 0.0)
        self.assertEqual(self.home.notes, 'match abandoned')

    def test_a_bet_with_no_result_stays_pending(self):
        settled, unresolved = settle_pending(self.ledger, {})
        self.assertEqual(settled, [])
        self.assertEqual(len(unresolved), 3)
        self.assertTrue(all(b.status == PENDING for b in self.ledger.bets))

    def test_ungradeable_selection_is_reported_not_graded(self):
        self.home.selection = 'correct_score_2_1'
        results = load_results(self.write_results(
            [{'fixture': 'Arsenal v Everton', 'score': '2-1'}]))
        settled, unresolved = settle_pending(self.ledger, results)

        self.assertEqual(settled, [])
        self.assertEqual(self.home.status, PENDING)
        self.assertIn('no grading rule', unresolved[0][1])

    def test_unreadable_score_leaves_the_bet_pending(self):
        results = load_results(self.write_results(
            [{'fixture': 'Arsenal v Everton', 'score': 'two nil'}]))
        settled, unresolved = settle_pending(self.ledger, results)
        self.assertEqual(settled, [])
        self.assertEqual(self.home.status, PENDING)

    def test_already_settled_bets_are_not_touched_again(self):
        results = load_results(self.write_results(
            [{'fixture': 'Arsenal v Everton', 'score': '2-0'}]))
        settle_pending(self.ledger, results)
        returned_first = self.home.returned

        settled, _ = settle_pending(self.ledger, results)
        self.assertEqual(settled, [])
        self.assertEqual(self.home.returned, returned_first)


class TestOverrides(SettlementTestCase):
    def test_set_settles_a_bet_by_hand(self):
        settled, _ = settle_pending(
            self.ledger, overrides={self.home.bet_id: VOID})
        self.assertEqual(self.home.status, VOID)
        self.assertIn('by hand', self.home.notes)

    def test_override_beats_the_results_file(self):
        results = load_results(self.write_results(
            [{'fixture': 'Arsenal v Everton', 'score': '2-0'}]))
        settle_pending(self.ledger, results,
                       overrides={self.home.bet_id: VOID})
        self.assertEqual(self.home.status, VOID)

    def test_override_for_an_unknown_bet_raises_before_anything_settles(self):
        results = load_results(self.write_results(
            [{'fixture': 'Spurs v Leeds', 'score': '1-1'}]))
        with self.assertRaises(KeyError):
            settle_pending(self.ledger, results, overrides={'99999': VOID})
        self.assertTrue(all(b.status == PENDING for b in self.ledger.bets))

    def test_override_for_an_already_settled_bet_raises(self):
        self.ledger.settle(self.home.bet_id, WON)
        with self.assertRaises(KeyError):
            settle_pending(self.ledger, overrides={self.home.bet_id: LOST})

    def test_parse_override(self):
        self.assertEqual(parse_override('00003=void'), ('00003', 'void'))
        self.assertEqual(parse_override('00003=WON'), ('00003', 'won'))

    def test_parse_override_rejects_a_non_settlement_status(self):
        import argparse
        for bad in ('00003=pending', '00003=', '=won', 'nonsense'):
            with self.assertRaises(argparse.ArgumentTypeError):
                parse_override(bad)


class TestScrapedMatchStore(SettlementTestCase):
    def write_matches(self, records):
        (self.data / 'matches_data.json').write_text(json.dumps(records))
        return index_matches(self.data / 'matches_data.json')

    def test_result_resolved_from_the_scraped_store(self):
        index = self.write_matches([
            {'date': '2026-05-24', 'home_team': 'Arsenal',
             'away_team': 'Everton', 'home_goals': 3, 'away_goals': 1},
        ])
        settled, _ = settle_pending(self.ledger, {}, index)
        self.assertEqual([(b.bet_id, s) for b, s in settled],
                         [(self.home.bet_id, WON)])
        self.assertIn('matches_data 2026-05-24', self.home.notes)

    def test_an_earlier_meeting_of_the_same_fixture_is_not_used(self):
        """A bet cannot be settled on a match played before it was struck."""
        bet = self._place('Fulham v Newcastle', 'home', 0.60, 2.10,
                          placed_at='2026-05-20 12:00:00')
        index = self.write_matches([
            {'date': '2025-11-02', 'home_team': 'Fulham',
             'away_team': 'Newcastle', 'home_goals': 2, 'away_goals': 0},
        ])
        self.assertIsNone(match_for_bet(bet, index))
        with self.assertRaises(Unresolved):
            resolve(bet, {}, index, {})
        self.assertEqual(bet.status, PENDING)

    def test_the_first_meeting_after_placement_is_used(self):
        bet = self._place('Fulham v Newcastle', 'home', 0.60, 2.10,
                          placed_at='2026-05-20 12:00:00')
        index = self.write_matches([
            {'date': '2027-05-01', 'home_team': 'Fulham',
             'away_team': 'Newcastle', 'home_goals': 0, 'away_goals': 4},
            {'date': '2026-05-24', 'home_team': 'Fulham',
             'away_team': 'Newcastle', 'home_goals': 2, 'away_goals': 0},
            {'date': '2025-11-02', 'home_team': 'Fulham',
             'away_team': 'Newcastle', 'home_goals': 1, 'away_goals': 1},
        ])
        self.assertEqual(match_for_bet(bet, index)['date'], '2026-05-24')

    def test_unplayed_records_are_not_treated_as_results(self):
        index = self.write_matches([
            {'date': '2026-05-24', 'home_team': 'Arsenal',
             'away_team': 'Everton', 'home_goals': None, 'away_goals': None},
        ])
        settled, unresolved = settle_pending(self.ledger, {}, index)
        self.assertEqual(settled, [])
        self.assertEqual(len(unresolved), 3)

    def test_the_results_file_wins_over_the_scraped_store(self):
        index = self.write_matches([
            {'date': '2026-05-24', 'home_team': 'Arsenal',
             'away_team': 'Everton', 'home_goals': 0, 'away_goals': 2},
        ])
        results = load_results(self.write_results(
            [{'fixture': 'Arsenal v Everton', 'score': '3-1'}]))
        settle_pending(self.ledger, results, index)
        self.assertEqual(self.home.status, WON)
        self.assertIn('results file', self.home.notes)


class TestRun(SettlementTestCase):
    def test_run_writes_the_ledger_and_populates_the_summary(self):
        self.write_results([
            {'fixture': 'Arsenal v Everton', 'score': '2-0'},
            {'fixture': 'Spurs v Leeds', 'score': '1-0'},
            {'fixture': 'Burnley v Wolves', 'status': 'void'},
        ])
        ledger, settled, unresolved = run(data_dir=self.data)

        self.assertEqual(len(settled), 3)
        self.assertEqual(unresolved, [])

        reloaded = Ledger(path=self.data / 'bankroll.json')
        summary = reloaded.summary()
        self.assertEqual(summary['bets_open'], 0)
        self.assertEqual(summary['wins'], 1)
        self.assertEqual(summary['losses'], 1)
        self.assertEqual(summary['bets_void'], 1)
        self.assertEqual(summary['strike_rate'], 0.5)
        self.assertIsNotNone(summary['roi'])
        self.assertIsNotNone(summary['roi_pct'])
        self.assertNotEqual(summary['realised_pnl'], 0.0)
        self.assertEqual(summary['roi_pct'],
                         round(summary['roi'] * 100, 2))

    def test_dry_run_grades_without_writing(self):
        self.write_results([{'fixture': 'Arsenal v Everton', 'score': '2-0'}])
        before = (self.data / 'bankroll.json').read_text()

        ledger, settled, _ = run(data_dir=self.data, dry_run=True)
        self.assertEqual(len(settled), 1)
        self.assertEqual((self.data / 'bankroll.json').read_text(), before)
        self.assertEqual(
            Ledger(path=self.data / 'bankroll.json').bets[0].status, PENDING)

    def test_run_needs_from_matches_to_read_the_scraped_store(self):
        (self.data / 'matches_data.json').write_text(json.dumps([
            {'date': '2026-05-24', 'home_team': 'Arsenal',
             'away_team': 'Everton', 'home_goals': 3, 'away_goals': 1},
        ]))
        _, settled, _ = run(data_dir=self.data)
        self.assertEqual(settled, [])

        _, settled, _ = run(data_dir=self.data, from_matches=True)
        self.assertEqual(len(settled), 1)

    def test_settlement_frees_the_staking_bankroll(self):
        """The point of settling: Kelly can size against the money again."""
        self.write_results([{'fixture': 'Arsenal v Everton', 'score': '2-0'}])
        committed_before = Ledger(path=self.data / 'bankroll.json').committed

        ledger, _, _ = run(data_dir=self.data)
        self.assertLess(ledger.committed, committed_before)
        self.assertGreater(ledger.bankroll, 1000.0)

    def test_run_on_an_empty_ledger_is_a_no_op(self):
        empty = Path(self.tmp.name) / 'empty'
        empty.mkdir()
        ledger, settled, unresolved = run(data_dir=empty)
        self.assertEqual(settled, [])
        self.assertFalse((empty / 'bankroll.json').exists())

    def test_alternate_results_path(self):
        path = self.write_results(
            [{'fixture': 'Arsenal v Everton', 'score': '2-0'}],
            name='fri_results.json')
        _, settled, _ = run(data_dir=self.data, results_path=path)
        self.assertEqual(len(settled), 1)


class TestCli(SettlementTestCase):
    def test_main_settles_from_the_default_results_file(self):
        self.write_results([{'fixture': 'Arsenal v Everton', 'score': '2-0'}])
        self.assertEqual(main(['--data', str(self.data)]), 0)
        self.assertEqual(
            Ledger(path=self.data / 'bankroll.json').bets[0].status, WON)

    def test_main_passes_set_through(self):
        code = main(['--data', str(self.data),
                     '--set', f'{self.home.bet_id}=void'])
        self.assertEqual(code, 0)
        self.assertEqual(
            Ledger(path=self.data / 'bankroll.json').bets[0].status, VOID)

    def test_main_reports_failure_without_writing(self):
        code = main(['--data', str(self.data), '--set', '99999=void'])
        self.assertEqual(code, 1)
        self.assertTrue(all(b.status == PENDING for b in
                            Ledger(path=self.data / 'bankroll.json').bets))

    def test_main_honours_dry_run(self):
        self.write_results([{'fixture': 'Arsenal v Everton', 'score': '2-0'}])
        self.assertEqual(
            main(['--data', str(self.data), '--dry-run']), 0)
        self.assertEqual(
            Ledger(path=self.data / 'bankroll.json').bets[0].status, PENDING)


if __name__ == '__main__':
    unittest.main(verbosity=2)
