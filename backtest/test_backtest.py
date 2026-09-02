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
from backtest import shrinkage                                 # noqa: E402
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


# --- shrinkage ------------------------------------------------------------

def test_shrinkage_default_is_the_unshrunk_model():
    """
    The default must be the model as it was, not merely close to it.

    1.0 * p + 0.0 * f is p exactly in IEEE-754, so this is an equality test
    and not a tolerance one. If it ever needs a tolerance, the default has
    stopped being the production model and every headline number in NOTES.md
    has silently moved.
    """
    matches = load_all()
    assert run(matches)['fixtures'] == run(matches, shrinkage=1.0)['fixtures']
    for row in run(matches)['fixtures']:
        assert row['model_prob'] == row['raw_model_prob']


def test_shrinkage_zero_is_the_devigged_market():
    for row in run(load_all(), shrinkage=0.0)['fixtures']:
        assert abs(row['model_prob'] - row['market_fair']) < 1e-6, row


def test_shrinkage_blends_linearly():
    w = 0.35
    for row in run(load_all(), shrinkage=w)['fixtures']:
        expected = w * row['raw_model_prob'] + (1 - w) * row['market_fair']
        assert abs(row['model_prob'] - expected) < 1e-6, row


def test_shrinkage_does_not_move_the_elo_chain():
    """
    Ratings, bands and the priced universe must be independent of w.

    This is the property the whole grid search rests on: it is what lets one
    Brier column be compared down twenty-one rows. `shrinkage.search` asserts
    it at runtime too -- checked here as well so a regression fails in the
    tests rather than mid-grid.
    """
    matches = load_all()
    baseline = run(matches)
    for w in (0.0, 0.5):
        other = run(matches, shrinkage=w)
        assert other['final_elo'] == baseline['final_elo']
        assert other['anomalies'] == baseline['anomalies']

        def universe(result):
            return {(r['date'], r['home_team'], r['book'], r['selection'],
                     r['raw_model_prob'], r['odds'], r['won'])
                    for r in result['fixtures']}

        assert universe(other) == universe(baseline), f'w={w} repriced'


def test_shrinkage_has_no_lookahead_either():
    """
    The same truncation test as the unshrunk run, at a w that changes bets.

    The blend reads the closing prices of one match, which the EV arithmetic
    already reads at that point in the walk. Nothing is pooled across matches
    or across dates -- so truncating the future must leave the past identical
    here exactly as it does at w=1.
    """
    matches = load_all()
    cutoff, w = '2024-06-01', 0.5
    full = run(matches, shrinkage=w)
    truncated = run([m for m in matches if m['date'] < cutoff], shrinkage=w)

    def key(row):
        return (row['date'], row['home_team'], row['book'], row['selection'])

    full_rows = {key(r): r for r in full['fixtures'] if r['date'] < cutoff}
    truncated_rows = {key(r): r for r in truncated['fixtures']}

    assert full_rows, 'expected priced fixtures before the cutoff'
    assert set(full_rows) == set(truncated_rows)
    for row_key, row in full_rows.items():
        assert row == truncated_rows[row_key], (
            f'truncating the future changed {row_key} at w={w}')


def test_shrinkage_weight_is_validated():
    for bad in (-0.01, 1.01):
        try:
            run(load_all()[:1], shrinkage=bad)
        except ValueError:
            continue
        raise AssertionError(f'shrinkage={bad} should have been rejected')


def test_brier_is_a_mean_squared_error():
    rows = [{'model_prob': 0.25, 'won': True},     # (0.25 - 1)^2 = 0.5625
            {'model_prob': 0.25, 'won': False}]    # (0.25 - 0)^2 = 0.0625
    assert abs(shrinkage.brier(rows) - 0.3125) < 1e-12
    assert shrinkage.brier([]) is None
    # A perfect forecaster scores 0, a maximally wrong one scores 1.
    assert shrinkage.brier([{'model_prob': 1.0, 'won': True}]) == 0.0
    assert shrinkage.brier([{'model_prob': 1.0, 'won': False}]) == 1.0


def test_analytic_best_w_finds_the_true_minimum():
    """The closed form must agree with a brute-force scan of the real data."""
    rows = run(load_all())['fixtures']

    def score_at(w):
        return shrinkage.brier(
            [{'model_prob': w * r['raw_model_prob']
              + (1 - w) * r['market_fair'], 'won': r['won']} for r in rows])

    exact = shrinkage.analytic_best_w(rows)
    assert exact is not None
    # Nothing on a fine scan around it may beat it.
    for step in range(-10, 11):
        candidate = exact + step * 0.01
        assert score_at(candidate) >= score_at(exact) - 1e-12, candidate


def test_analytic_best_w_is_undefined_when_model_equals_market():
    """No deviation to weight means no weight to solve for -- not a zero."""
    rows = [{'raw_model_prob': 0.4, 'market_fair': 0.4, 'won': True},
            {'raw_model_prob': 0.6, 'market_fair': 0.6, 'won': False}]
    assert shrinkage.analytic_best_w(rows) is None


def test_grid_points_are_exact():
    grid = shrinkage.grid_points(0.05)
    assert len(grid) == 21
    assert grid[0] == 0.0 and grid[-1] == 1.0
    assert grid[3] == 0.15, 'float drift would put 0.15000000000000002 here'
    assert shrinkage.grid_points(0.25) == [0.0, 0.25, 0.5, 0.75, 1.0]


def test_grid_population_check_actually_catches_a_change():
    """The runtime guard must fail on a changed universe, not wave it past."""
    rows = [{'date': '2022-01-01', 'home_team': 'A', 'away_team': 'B',
             'book': 'avg', 'selection': 'home', 'raw_model_prob': 0.5,
             'odds': 2.0}]
    reference = {(r['date'], r['home_team'], r['away_team'], r['book'],
                  r['selection'], r['raw_model_prob'], r['odds'])
                 for r in rows}
    shrinkage._assert_population_is_stable(reference, rows, 0.5)  # no raise

    changed = [dict(rows[0], raw_model_prob=0.6)]
    try:
        shrinkage._assert_population_is_stable(reference, changed, 0.5)
    except RuntimeError:
        return
    raise AssertionError('a changed universe passed the stability check')


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
