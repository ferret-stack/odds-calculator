"""
Tests for the results autofill tool.

The tool exists to stop a scoreline being retyped by hand, so what matters is
that it never writes something settlement would grade differently from the
scrape it claims to have read, and never overwrites the operator's own ruling
by accident: an entry already in the file is kept, a fixture with no scrape on
or after the bet date is reported rather than invented, and the file's own
`_comment` block survives a rewrite.
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.bankroll import Ledger
from pipeline.staking import size_bet
from tools.fill_results import collect, load_document, main, merge, run

MATCHES = [
    {'match_id': 1, 'date': '2025-08-16', 'home_team': 'Arsenal',
     'away_team': 'Everton', 'home_goals': 4, 'away_goals': 0},
    {'match_id': 2, 'date': '2026-08-22', 'home_team': 'Arsenal',
     'away_team': 'Everton', 'home_goals': 1, 'away_goals': 2},
    {'match_id': 3, 'date': '2026-08-22', 'home_team': 'Spurs',
     'away_team': 'Leeds', 'home_goals': 0, 'away_goals': 0},
    {'match_id': 4, 'date': '2026-08-23', 'home_team': 'Chelsea',
     'away_team': 'Fulham', 'home_goals': 2, 'away_goals': 1},
]


def make_ledger(path, fixtures, placed_at='2026-08-20 10:00:00'):
    ledger = Ledger(path=path, starting_bankroll=1000)
    for fixture in fixtures:
        decision = size_bet(fixture, '1x2', 'home', 0.60, 2.50, 1000)
        ledger.place(decision, placed_at=placed_at)
    ledger.save()
    return ledger


class FillResultsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name)
        (self.data / 'matches_data.json').write_text(json.dumps(MATCHES))

    def results(self):
        return json.loads((self.data / 'results.json').read_text())

    def test_writes_the_scraped_scoreline_for_a_pending_bet(self):
        make_ledger(self.data / 'bankroll.json', ['Arsenal v Everton'])

        added, kept, replaced, missing = run(data_dir=self.data)

        self.assertEqual([e['score'] for e in added], ['1-2'])
        self.assertEqual(([], [], []), (kept, replaced, missing))
        entry, = self.results()['results']
        self.assertEqual('Arsenal v Everton', entry['fixture'])
        self.assertEqual('1-2', entry['score'])
        self.assertEqual('2026-08-22', entry['date'])
        self.assertIn('match_id 2', entry['source'])

    def test_ignores_a_meeting_played_before_the_bet_was_struck(self):
        """The 2025 Arsenal v Everton is a different match, not this bet's."""
        make_ledger(self.data / 'bankroll.json', ['Arsenal v Everton'],
                    placed_at='2026-08-20 10:00:00')

        added, _, _, missing = run(data_dir=self.data)

        self.assertEqual([], missing)
        self.assertEqual('1-2', added[0]['score'])

    def test_fixture_with_no_scrape_is_reported_not_invented(self):
        make_ledger(self.data / 'bankroll.json',
                    ['Arsenal v Everton', 'Hull City v Man Utd'])

        added, _, _, missing = run(data_dir=self.data)

        self.assertEqual(['Arsenal v Everton'],
                         [e['fixture'] for e in added])
        self.assertEqual([('Hull City v Man Utd', '2026-08-20')], missing)
        self.assertEqual(['Arsenal v Everton'],
                         [e['fixture'] for e in self.results()['results']])

    def test_existing_entry_is_kept_and_overwrite_replaces_it(self):
        make_ledger(self.data / 'bankroll.json', ['Arsenal v Everton'])
        (self.data / 'results.json').write_text(json.dumps({
            '_comment': ['keep me'],
            'results': [{'fixture': 'arsenal  v  everton', 'score': '3-3',
                         'source': 'typed by hand'}],
        }))

        added, kept, replaced, _ = run(data_dir=self.data)
        self.assertEqual(([], []), (added, replaced))
        self.assertEqual(1, len(kept))
        self.assertEqual('3-3', self.results()['results'][0]['score'])
        self.assertEqual(['keep me'], self.results()['_comment'])

        added, kept, replaced, _ = run(data_dir=self.data, overwrite=True)
        self.assertEqual(([], []), (added, kept))
        self.assertEqual('1-2', self.results()['results'][0]['score'])
        self.assertEqual(1, len(self.results()['results']))
        self.assertEqual(['keep me'], self.results()['_comment'])

    def test_dry_run_writes_nothing(self):
        make_ledger(self.data / 'bankroll.json', ['Arsenal v Everton'])

        added, _, _, _ = run(data_dir=self.data, dry_run=True)

        self.assertEqual(1, len(added))
        self.assertFalse((self.data / 'results.json').exists())

    def test_all_widens_to_every_scraped_fixture_from_the_bet_date(self):
        make_ledger(self.data / 'bankroll.json', ['Arsenal v Everton'])

        added, _, _, _ = run(data_dir=self.data, include_all=True)

        self.assertEqual(
            {'Arsenal v Everton', 'Spurs v Leeds', 'Chelsea v Fulham'},
            {e['fixture'] for e in added})

    def test_no_pending_bets_is_a_no_op(self):
        Ledger(path=self.data / 'bankroll.json', starting_bankroll=1000).save()

        self.assertEqual(([], [], [], []), run(data_dir=self.data))
        self.assertFalse((self.data / 'results.json').exists())

    def test_bare_list_document_is_accepted(self):
        make_ledger(self.data / 'bankroll.json', ['Arsenal v Everton'])
        (self.data / 'results.json').write_text(json.dumps(
            [{'home_team': 'Spurs', 'away_team': 'Leeds', 'score': '1-1'}]))

        run(data_dir=self.data)

        document = self.results()
        self.assertIsInstance(document, list)
        self.assertEqual(2, len(document))

    def test_merge_collapses_duplicate_new_entries(self):
        entries = []
        entry = {'fixture': 'Arsenal v Everton', 'score': '1-2'}
        added, kept, replaced = merge(
            entries, [('arsenal v everton', entry),
                      ('arsenal v everton', dict(entry, score='9-9'))])
        self.assertEqual([entry], added)
        self.assertEqual(([], []), (kept, replaced))

    def test_cli_exit_codes(self):
        make_ledger(self.data / 'bankroll.json', ['Arsenal v Everton'])
        self.assertEqual(0, main(['--data', str(self.data), '--dry-run']))

        # A results file that is not a document: reported, not written over.
        (self.data / 'results.json').write_text('{"results": {}}')
        self.assertEqual(1, main(['--data', str(self.data)]))


class SettlementHandoffTest(unittest.TestCase):
    """What the tool writes is what settlement then grades."""

    def test_filled_document_settles_the_book(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            (data / 'matches_data.json').write_text(json.dumps(MATCHES))
            make_ledger(data / 'bankroll.json', ['Arsenal v Everton'])

            run(data_dir=data)

            from pipeline.settle_results import run as settle
            _, settled, unresolved = settle(data_dir=data)

            self.assertEqual([], unresolved)
            (bet, status), = settled
            self.assertEqual('lost', status)     # home bet, 1-2
            self.assertIn('1-2', bet.notes)


if __name__ == '__main__':
    unittest.main()
