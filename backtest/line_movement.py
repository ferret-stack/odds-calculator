#!/usr/bin/env python3
"""
Line-movement proxy: does the model's disagreement with the pre-closing
price predict the move to the closing price?

    python3 backtest/line_movement.py --league E2 E3 --pool
    python3 backtest/line_movement.py --league E1 EC E0

Prints only. Writes no files. Places no bets: it uses every match priced in
both families, not just the ones the staking rule selected. The 2025/26
holdout cannot be reached (same loader, same guard).

DECISIONS, FIXED BEFORE ANY RESULT (2026-11-15)
-----------------------------------------------
Data      Same chains as the lower-league backtest: per-league ELO, league
          seeding, walk-forward bands, seasons 2019/20-2024/25. Families
          'avg' (pre-closing) and 'avgc' (closing) loaded together. A match
          is used only if it has all three prices in BOTH families; matches
          priced in one family only are counted and excluded. The model
          probability must be identical in both families' fixture rows, or
          the run stops.
Variables One row per match and selection (three per match):
              m = market_fair from Avg   (proportional de-vig, the engine's)
              c = market_fair from AvgC
              p = model probability (no floor: nothing here takes a log)
              d = p - m      disagreement
              y = c - m      move to closing
Model     PRIMARY regression A, OLS:
              y = a_home + a_draw + a_away + g*m + beta*d + e
          i.e. does disagreement predict the move once selection and price
          level are held fixed. Proportional de-vigging of a lower-margin
          closing book, or a systematic pre-kickoff drift on draws or
          longshots, moves fair probabilities by price level; the model's
          disagreement is correlated with price level, so without these
          controls either would read as signal.
          Descriptive regression B: y = a + beta*d (a is zero by
          construction, since d and y each sum to zero within a match).
Errors    Cluster-robust (sandwich, CR1) standard errors clustered BY MATCH,
          which absorbs the exact dependence of a match's three rows.
          Descriptive sensitivity: the same fit clustered BY DATE.
          95% CI = beta +/- 1.96 se.
Reading   PRIMARY: E2 + E3 pooled, regression A, clustered by match.
          SIGNAL    = the 95% CI on beta lies entirely above zero.
          NO SIGNAL = anything else, including a CI spanning zero or one
                      entirely below it.
          E2 and E3 separately, E0, E1, EC, regression B, date clustering,
          sign agreement and mean |y| are descriptive only.
Meaning   SIGNAL: the model's disagreement with the pre-closing price partly
          anticipates the close, so a forward CLV log is worth running. It
          is NOT evidence of profit; the backtest and beta results stand.
          NO SIGNAL: the model does not anticipate the closing line either;
          this proxy is closed.
          Scale: beta is in probability units; beta = 0.1 means the close
          moves 10% of the way toward the model.
"""

import argparse
import math
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backtest.data import (  # noqa: E402
    LEAGUES, entrant_directions, load_league, season_label)
from backtest.diagnostic import _solve  # noqa: E402
from backtest.engine import run  # noqa: E402

BET_YEARS = tuple(range(2020, 2026))          # 2019/20 .. 2024/25
FAMILIES = ('avg', 'avgc')
OUTCOMES = ('home', 'draw', 'away')
Z = 1.96
MODEL_PROB_TOLERANCE = 1e-9

# Column layouts. Each row dict carries 'm', 'd', 'selection'.
REGRESSION_A = ('a_home', 'a_draw', 'a_away', 'm', 'd')
REGRESSION_B = ('const', 'd')


def observations(fixtures, league):
    """
    Rows for matches priced in both families, plus the exclusion counts.

    Returns (rows, excluded) where rows are dicts with league, season, date,
    match, selection, m, c, p, d, y and excluded is
    {'avg_only': n, 'avgc_only': n}.
    """
    by_match = {}
    for row in fixtures:
        if row['book'] not in FAMILIES:
            continue
        key = (row['date'], row['home_team'], row['away_team'])
        entry = by_match.setdefault(key, {'season': row['season']})
        entry.setdefault(row['book'], {})[row['selection']] = row

    rows, excluded = [], {'avg_only': 0, 'avgc_only': 0}
    for key in sorted(by_match):
        entry = by_match[key]
        have = [f for f in FAMILIES if f in entry]
        if have == ['avg']:
            excluded['avg_only'] += 1
            continue
        if have == ['avgc']:
            excluded['avgc_only'] += 1
            continue
        for family in FAMILIES:
            if set(entry[family]) != set(OUTCOMES):
                raise ValueError(f'incomplete 1x2 in {family} for {key}')
        for selection in OUTCOMES:
            pre = entry['avg'][selection]
            close = entry['avgc'][selection]
            if abs(pre['model_prob'] - close['model_prob']) > MODEL_PROB_TOLERANCE:
                raise ValueError(f'model probability differs between '
                                 f'families for {key} {selection}')
            if pre['market_fair'] <= 0 or close['market_fair'] <= 0:
                raise ValueError(f'no market probability for {key}')
            m, c, p = pre['market_fair'], close['market_fair'], pre['model_prob']
            rows.append({'league': league, 'season': entry['season'],
                         'date': key[0], 'match': (league,) + key,
                         'selection': selection,
                         'm': m, 'c': c, 'p': p, 'd': p - m, 'y': c - m})
    return rows, excluded


def _design(row, columns):
    values = {'const': 1.0, 'm': row['m'], 'd': row['d']}
    for selection in OUTCOMES:
        values[f'a_{selection}'] = 1.0 if row['selection'] == selection else 0.0
    return [values[c] for c in columns]


def _inverse(matrix):
    n = len(matrix)
    columns = [_solve(matrix, [1.0 if i == j else 0.0 for i in range(n)])
               for j in range(n)]
    return [[columns[j][i] for j in range(n)] for i in range(n)]


def ols_clustered(rows, columns, cluster):
    """
    OLS of y on `columns`, CR1 cluster-robust covariance.

    cluster -- function row -> cluster key.
    Returns {'coef': {name: value}, 'se': {name: value}, 'n', 'clusters'}.
    """
    k = len(columns)
    xtx = [[0.0] * k for _ in range(k)]
    xty = [0.0] * k
    design = []
    for row in rows:
        x = _design(row, columns)
        design.append(x)
        for i in range(k):
            xty[i] += x[i] * row['y']
            for j in range(k):
                xtx[i][j] += x[i] * x[j]
    beta = _solve(xtx, xty)
    bread = _inverse(xtx)

    scores = {}
    for row, x in zip(rows, design):
        resid = row['y'] - sum(b * v for b, v in zip(beta, x))
        acc = scores.setdefault(cluster(row), [0.0] * k)
        for i in range(k):
            acc[i] += x[i] * resid
    meat = [[0.0] * k for _ in range(k)]
    for s in scores.values():
        for i in range(k):
            for j in range(k):
                meat[i][j] += s[i] * s[j]

    n, g = len(rows), len(scores)
    if g < 2 or n <= k:
        raise ValueError('too few observations or clusters')
    scale = (g / (g - 1)) * ((n - 1) / (n - k))
    half = [[sum(bread[i][a] * meat[a][j] for a in range(k)) for j in range(k)]
            for i in range(k)]
    cov = [[scale * sum(half[i][a] * bread[a][j] for a in range(k))
            for j in range(k)] for i in range(k)]
    return {'coef': dict(zip(columns, beta)),
            'se': {c: math.sqrt(cov[i][i]) for i, c in enumerate(columns)},
            'n': n, 'clusters': g}


def by_match(row):
    return row['match']


def by_date(row):
    return row['date']


def analyse(rows):
    """Every number the proxy reports, for one set of rows."""
    primary = ols_clustered(rows, REGRESSION_A, by_match)
    by_day = ols_clustered(rows, REGRESSION_A, by_date)
    simple = ols_clustered(rows, REGRESSION_B, by_match)
    signed = [r for r in rows if r['d'] != 0 and r['y'] != 0]
    agree = sum((r['d'] > 0) == (r['y'] > 0) for r in signed)

    def ci(fit):
        b, se = fit['coef']['d'], fit['se']['d']
        return (b - Z * se, b + Z * se)

    return {
        'matches': primary['clusters'], 'rows': len(rows),
        'dates': by_day['clusters'],
        'mean_abs_d': sum(abs(r['d']) for r in rows) / len(rows),
        'mean_abs_y': sum(abs(r['y']) for r in rows) / len(rows),
        'sign_agreement': agree / len(signed) if signed else None,
        'beta': primary['coef']['d'], 'beta_se': primary['se']['d'],
        'beta_ci': ci(primary),
        'gamma': primary['coef']['m'], 'gamma_se': primary['se']['m'],
        'beta_se_date': by_day['se']['d'], 'beta_ci_date': ci(by_day),
        'beta_simple': simple['coef']['d'], 'beta_simple_se': simple['se']['d'],
        'beta_simple_ci': ci(simple),
    }


def verdict(result):
    return 'SIGNAL' if result['beta_ci'][0] > 0 else 'NO SIGNAL'


def league_rows(code, years=BET_YEARS):
    """Load one league, run the chain, return (rows, excluded)."""
    matches, _skips = load_league(code, bet_seasons=years, families=FAMILIES)
    result = run(matches, seeding='league', league=code,
                 directions=entrant_directions(code),
                 bet_seasons=[season_label(y) for y in years], books=FAMILIES)
    return observations(result['fixtures'], code)


def _print(label, result, excluded):
    low, high = result['beta_ci']
    dlow, dhigh = result['beta_ci_date']
    slow, shigh = result['beta_simple_ci']
    print(f'{label}: {result["matches"]:,} matches ({result["rows"]:,} rows, '
          f'{result["dates"]:,} dates) | excluded: Avg only '
          f'{excluded["avg_only"]}, AvgC only {excluded["avgc_only"]}')
    print(f'  mean |d| {result["mean_abs_d"]:.4f} | mean |y| '
          f'{result["mean_abs_y"]:.4f} | sign agreement '
          f'{result["sign_agreement"]:.3f}')
    print(f'  A (selection + m controls, match clusters): beta = '
          f'{result["beta"]:.4f} (se {result["beta_se"]:.4f}), 95% CI '
          f'{low:.4f} to {high:.4f} | g on m = {result["gamma"]:.4f} '
          f'(se {result["gamma_se"]:.4f})')
    print(f'  A, date clusters: se {result["beta_se_date"]:.4f}, 95% CI '
          f'{dlow:.4f} to {dhigh:.4f}')
    print(f'  B (simple slope, match clusters): beta = '
          f'{result["beta_simple"]:.4f} (se {result["beta_simple_se"]:.4f}), '
          f'95% CI {slow:.4f} to {shigh:.4f}')
    print(f'  beta reading (A): {verdict(result)}')
    print()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--league', nargs='+', choices=LEAGUES, required=True)
    parser.add_argument('--pool', action='store_true',
                        help='also fit all the --league codes together')
    args = parser.parse_args(argv)

    labels = [season_label(y) for y in BET_YEARS]
    print(f'Families: avg -> avgc | seasons {labels[0]} .. {labels[-1]} | '
          f'd = model - Avg fair, y = AvgC fair - Avg fair')
    print()

    pooled, pooled_excluded = [], {'avg_only': 0, 'avgc_only': 0}
    for code in args.league:
        rows, excluded = league_rows(code)
        pooled += rows
        for k in pooled_excluded:
            pooled_excluded[k] += excluded[k]
        _print(code, analyse(rows), excluded)
    if args.pool and len(args.league) > 1:
        _print('POOLED ' + '+'.join(args.league), analyse(pooled),
               pooled_excluded)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
