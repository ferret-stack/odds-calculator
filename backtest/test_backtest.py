#!/usr/bin/env python3
"""
Self-checks for the backtest harness.

pytest-compatible (matching tests/), and runnable directly with
`python3 backtest/test_backtest.py` where pytest is not installed.

These check the properties the ROI numbers depend on: that the EV arithmetic
is production's, that settlement is right, and -- the one that matters most --
that the walk-forward genuinely does not see the future.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backtest import report                                    # noqa: E402
from backtest.data import load_all                             # noqa: E402
from backtest.engine import MARKETS, SETTLES, band_is_degenerate, run  # noqa: E402
from elo_calculator import calculate_elo_bands, elo_band       # noqa: E402
from pipeline.staking import expected_value                    # noqa: E402


def test_dataset_is_complete():
    matches = load_all()
    assert len(matches) == 2280, 'six PL seasons, 380 matches each'
    assert sorted({m['season'] for m in matches}) == [
        '2020-21', '2021-22', '2022-23', '2023-24', '2024-25', '2025-26']
    assert matches == sorted(matches, key=lambda m: (m['date'], m['time'],
                                                     m['home_team']))


def test_edge_matches_production_ev_formula():
    """edge_pct must be exactly staking.expected_value, not a re-derivation."""
    result = run(load_all())
    for row in result['fixtures'][:2000]:
        expected = expected_value(row['model_prob'], row['odds']) * 100
        assert abs(row['edge_pct'] - expected) < 1e-6, row


def test_settlement_follows_the_result():
    result = run(load_all())
    for row in result['fixtures']:
        assert row['won'] == (row['ftr'] == SETTLES[row['selection']])
        if not row['bet']:
            assert row['profit'] == 0.0
        elif row['won']:
            assert row['profit'] > 0
        else:
            assert row['profit'] == -row['stake']


def test_no_lookahead_in_the_band_table():
    """
    The strongest statement of the property: truncating the future must not
    change the past.

    A fixture priced on 2023-01-01 must get the same probability, edge and
    stake whether or not the 2024-25 and 2025-26 seasons are present in the
    input. If any future match leaked into the band table -- or into the ELO
    chain, or the promoted-team seeding -- the truncated run would disagree.
    """
    matches = load_all()
    cutoff = '2024-06-01'
    full = run(matches)
    truncated = run([m for m in matches if m['date'] < cutoff])

    def key(row):
        return (row['date'], row['home_team'], row['book'], row['selection'])

    full_rows = {key(r): r for r in full['fixtures'] if r['date'] < cutoff}
    truncated_rows = {key(r): r for r in truncated['fixtures']}

    assert full_rows, 'expected priced fixtures before the cutoff'
    assert set(full_rows) == set(truncated_rows), (
        'truncating the future changed WHICH selections were priced')
    for row_key, row in full_rows.items():
        assert row == truncated_rows[row_key], (
            f'truncating the future changed {row_key}')


def test_chain_is_deterministic():
    matches = load_all()
    first = run(matches)
    second = run(matches)
    assert first['fixtures'] == second['fixtures']
    assert first['final_elo'] == second['final_elo']


def test_degenerate_band_is_detected():
    """An all-even band prices to 0/0/0 and must not reach calculate_fair_odds."""
    assert band_is_degenerate({})
    assert band_is_degenerate({'stronger_win_pct': 0.0, 'draw_pct': 0.0,
                               'weaker_win_pct': 0.0})
    assert not band_is_degenerate({'stronger_win_pct': 0.4, 'draw_pct': 0.0,
                                   'weaker_win_pct': 0.6})


def test_probabilities_are_a_normalised_1x2():
    result = run(load_all())
    by_fixture = {}
    for row in result['fixtures']:
        if row['book'] != 'avg':
            continue
        by_fixture.setdefault((row['date'], row['home_team']), {})[
            row['selection']] = row['model_prob']
    complete = [p for p in by_fixture.values() if len(p) == len(MARKETS)]
    assert complete, 'expected fixtures priced on all three selections'
    for probs in complete:
        # calculate_fair_odds rounds each leg to 4dp, so the sum can miss 1.0
        # by up to 1.5e-4. Anything larger means the normalisation is wrong.
        assert abs(sum(probs.values()) - 1.0) < 2e-4, probs


def test_yield_and_flat_roi_agree_on_sign_for_a_known_set():
    rows = [
        {'stake': 10.0, 'profit': 10.0, 'won': True, 'odds': 2.0,
         'model_prob': 0.6, 'market_fair': 0.5, 'edge_pct': 20.0},
        {'stake': 10.0, 'profit': -10.0, 'won': False, 'odds': 2.0,
         'model_prob': 0.6, 'market_fair': 0.5, 'edge_pct': 20.0},
    ]
    summary = report.summarise(rows, 'test')
    assert summary['bets'] == 2
    assert summary['staked'] == 20.0
    assert summary['profit'] == 0.0
    assert summary['yield_pct'] == 0.0
    assert summary['flat_roi_pct'] == 0.0
    assert summary['strike_rate_pct'] == 50.0


def test_edge_buckets_partition_the_floor():
    assert report.edge_bucket(5.0) == '5-10%'
    assert report.edge_bucket(9.999) == '5-10%'
    assert report.edge_bucket(10.0) == '10-20%'
    assert report.edge_bucket(19.999) == '10-20%'
    assert report.edge_bucket(20.0) == '20%+'
    assert report.edge_bucket(4.99) == 'below floor'


def test_production_is_not_written_to():
    """
    A full run must leave every production file byte-identical.

    Checked by content hash rather than by reading the source for suspicious
    strings, so it holds however the harness is refactored.
    """
    import hashlib

    watched = sorted(
        [p for p in (REPO_ROOT / 'data').glob('*.json')]
        + [p for p in (REPO_ROOT / 'pipeline').glob('*.py')]
        + [p for p in (REPO_ROOT / 'tools').glob('*.py')]
        + [REPO_ROOT / 'elo_calculator.py']
        + [p for p in (REPO_ROOT / 'docs' / 'historical').glob('*.csv')])
    assert watched, 'expected production files to watch'

    def digest():
        return {p: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in watched if p.exists()}

    before = digest()
    run(load_all())
    assert digest() == before, 'a production file changed during a run'


if __name__ == '__main__':
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith('test_') or not callable(fn):
            continue
        try:
            fn()
            print(f'  PASS  {name}')
        except AssertionError as exc:
            failures += 1
            print(f'  FAIL  {name}: {exc}')
    print(f'\n{failures} failure(s)')
    raise SystemExit(1 if failures else 0)
