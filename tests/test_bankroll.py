"""Tests for the bankroll ledger."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.bankroll import LOST, PENDING, VOID, WON, Ledger
from pipeline.staking import size_bet


class LedgerTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'bankroll.json'
        self.ledger = Ledger(path=self.path, starting_bankroll=1000.0)

    def tearDown(self):
        self.tmp.cleanup()

    def _place(self, p=0.55, odds=2.10):
        decision = size_bet('A v B', '1x2', 'home', p, odds,
                            self.ledger.staking_bankroll)
        return self.ledger.place(decision)


class TestPlacing(LedgerTestCase):
    def test_place_records_the_bet(self):
        bet = self._place()
        self.assertEqual(bet.stake, 35.23)
        self.assertEqual(bet.status, PENDING)
        self.assertEqual(len(self.ledger.bets), 1)

    def test_cannot_place_a_rejected_decision(self):
        rejected = size_bet('A v B', '1x2', 'home', 0.40, 2.00, 1000.0)
        with self.assertRaises(ValueError):
            self.ledger.place(rejected)

    def test_open_stake_is_committed_and_excluded_from_staking_balance(self):
        bet = self._place()
        self.assertEqual(self.ledger.committed, bet.stake)
        self.assertEqual(self.ledger.bankroll, 1000.0)
        self.assertEqual(self.ledger.staking_bankroll, 1000.0 - bet.stake)

    def test_second_bet_sizes_off_the_reduced_balance(self):
        """Kelly is a fraction of bankroll, so committed money must not count."""
        first = self._place()
        second = self._place()
        self.assertLess(second.stake, first.stake)


class TestBetIds(LedgerTestCase):
    """
    A bet id is how settlement and `--set` name a bet, so it has to be unique
    for a correction to be aimable. The original scheme numbered a new bet
    `len(self.bets) + 1`, which reissues a live id as soon as the sequence has
    a gap in it -- as one does after a record is removed by hand.
    """

    def test_ids_are_sequential_on_an_unbroken_ledger(self):
        self.assertEqual(['00001', '00002', '00003'],
                         [self._place().bet_id for _ in range(3)])

    def test_a_gap_does_not_reissue_a_live_id(self):
        ids = [self._place().bet_id for _ in range(3)]
        # The operator deletes the middle record by hand and the file is
        # reloaded: the sequence now runs 00001, 00003.
        del self.ledger.bets[1]
        self.ledger.save()

        reloaded = Ledger(path=self.path)
        decision = size_bet('C v D', '1x2', 'home', 0.55, 2.10, 1000.0)
        placed = reloaded.place(decision)

        self.assertEqual('00004', placed.bet_id)
        self.assertNotIn(placed.bet_id, ids[:1] + ids[2:])
        self.assertEqual(3, len({b.bet_id for b in reloaded.bets}))

    def test_hand_set_ids_are_left_alone_and_counted_around(self):
        self.ledger.place(
            size_bet('A v B', '1x2', 'home', 0.55, 2.10, 1000.0),
            bet_id='opening-bet')
        self.assertEqual('00001', self._place().bet_id)

    def test_a_duplicated_id_refuses_to_load(self):
        """Mis-settling on the wrong bet must not be possible silently."""
        self._place()
        self._place()
        self.ledger.save()
        payload = json.loads(self.path.read_text())
        payload['bets'][1]['bet_id'] = payload['bets'][0]['bet_id']
        self.path.write_text(json.dumps(payload))

        with self.assertRaises(ValueError) as caught:
            Ledger(path=self.path)
        self.assertIn('duplicate bet id', str(caught.exception))


class TestSettling(LedgerTestCase):
    def test_win_pays_stake_times_odds(self):
        bet = self._place()
        self.ledger.settle(bet.bet_id, WON)
        self.assertEqual(bet.returned, round(bet.stake * 2.10, 2))
        self.assertEqual(bet.profit, round(bet.stake * 1.10, 2))
        self.assertEqual(self.ledger.bankroll, 1000.0 + bet.profit)

    def test_loss_costs_the_stake(self):
        bet = self._place()
        self.ledger.settle(bet.bet_id, LOST)
        self.assertEqual(bet.profit, -bet.stake)
        self.assertEqual(self.ledger.bankroll, round(1000.0 - bet.stake, 2))

    def test_void_is_pnl_neutral(self):
        bet = self._place()
        self.ledger.settle(bet.bet_id, VOID)
        self.assertEqual(bet.profit, 0.0)
        self.assertEqual(self.ledger.bankroll, 1000.0)

    def test_settling_frees_the_commitment(self):
        bet = self._place()
        self.ledger.settle(bet.bet_id, WON)
        self.assertEqual(self.ledger.committed, 0.0)
        self.assertEqual(self.ledger.staking_bankroll, self.ledger.bankroll)

    def test_cannot_settle_twice(self):
        bet = self._place()
        self.ledger.settle(bet.bet_id, WON)
        with self.assertRaises(ValueError):
            self.ledger.settle(bet.bet_id, LOST)

    def test_unknown_bet_id_rejected(self):
        with self.assertRaises(KeyError):
            self.ledger.settle('nope', WON)

    def test_invalid_status_rejected(self):
        bet = self._place()
        with self.assertRaises(ValueError):
            self.ledger.settle(bet.bet_id, 'maybe')


class TestSummary(LedgerTestCase):
    def test_roi_and_strike_rate(self):
        a, b = self._place(), self._place()
        self.ledger.settle(a.bet_id, WON)
        self.ledger.settle(b.bet_id, LOST)

        summary = self.ledger.summary()
        self.assertEqual(summary['wins'], 1)
        self.assertEqual(summary['losses'], 1)
        self.assertEqual(summary['strike_rate'], 0.5)
        expected_roi = self.ledger.realised_pnl / (a.stake + b.stake)
        self.assertAlmostEqual(summary['roi'], round(expected_roi, 4), places=4)

    def test_voids_excluded_from_strike_rate_denominator(self):
        a, b, c = self._place(), self._place(), self._place()
        self.ledger.settle(a.bet_id, WON)
        self.ledger.settle(b.bet_id, LOST)
        self.ledger.settle(c.bet_id, VOID)

        summary = self.ledger.summary()
        self.assertEqual(summary['bets_void'], 1)
        self.assertEqual(summary['strike_rate'], 0.5)

    def test_empty_ledger_reports_none_not_zero(self):
        summary = self.ledger.summary()
        self.assertIsNone(summary['strike_rate'])
        self.assertIsNone(summary['roi'])


class TestPersistence(LedgerTestCase):
    def test_roundtrip(self):
        bet = self._place()
        self.ledger.settle(bet.bet_id, WON)
        self.ledger.save()

        reloaded = Ledger(path=self.path)
        self.assertEqual(reloaded.starting_bankroll, 1000.0)
        self.assertEqual(len(reloaded.bets), 1)
        self.assertEqual(reloaded.bankroll, self.ledger.bankroll)
        self.assertEqual(reloaded.bets[0].status, WON)


if __name__ == '__main__':
    unittest.main(verbosity=2)
