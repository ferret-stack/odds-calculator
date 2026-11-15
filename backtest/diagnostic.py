#!/usr/bin/env python3
"""
Diagnostic: does the ELO band model know anything the market price does not?

    python3 backtest/diagnostic.py --league E2 E3 --pool
    python3 backtest/diagnostic.py --league E1 EC E0
    python3 backtest/diagnostic.py --league E2 E3 --odds avgc --pool

Prints only. Writes no files. Places no bets: it uses every priced match,
not just the ones the staking rule selected. The 2025/26 holdout cannot be
reached (same loader, same guard).

DECISIONS, FIXED BEFORE ANY RESULT (2026-11-15)
-----------------------------------------------
Data      Same chains as the lower-league backtest: per-league ELO, league
          seeding, walk-forward bands, seasons 2019/20-2024/25. One
          observation per match that has all three prices in the family.
Inputs    m = market probability, de-vigged proportionally (the engine's
          `market_fair`).  p = model probability, floored at MODEL_PROB_FLOOR
          (the band table can emit an exact 0, whose log is undefined).
Model     Conditional logit over the three outcomes of each match:

              P(outcome k)  proportional to  exp(a*ln m_k + b*ln p_k)

          a = 1, b = 0 is "the market is right and the model adds nothing".
          Fitted by maximum likelihood; standard errors from the observed
          information; matches treated as independent.
Reading   PRIMARY: E2 + E3 pooled, Avg (pre-closing).
          SIGNAL    = the 95% CI on b lies entirely above zero.
          NO SIGNAL = anything else, including a CI that spans zero.
          E0, E1, EC and AvgC are descriptive only.
Meaning   NO SIGNAL closes the 1x2 band model as a source of edge.
          SIGNAL is NOT evidence of profit -- it says only that a blended or
          recalibrated model is worth its own pre-registered betting test,
          to be confirmed on the holdout.
Secondary Out-of-sample check, descriptive: fit on 2019/20-2021/22, score
          mean log loss on 2022/23-2024/25, market-only (a fitted, b = 0)
          against market + model. Reported as the difference; it decides
          nothing by itself.
"""

import argparse
import math
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backtest.data import (  # noqa: E402
    LEAGUES, ODDS_FAMILIES, entrant_directions, load_league, season_label)
from backtest.engine import run  # noqa: E402

MODEL_PROB_FLOOR = 0.01
BET_YEARS = tuple(range(2020, 2026))          # 2019/20 .. 2024/25
TRAIN_SEASONS = ('2019-20', '2020-21', '2021-22')
TEST_SEASONS = ('2022-23', '2023-24', '2024-25')
OUTCOMES = ('home', 'draw', 'away')
RESULT = {'H': 0, 'D': 1, 'A': 2}


def observations(fixtures, book):
    """
    One observation per match: (season, [(ln m, ln p) x3], winner index).

    Built from the engine's fixture log, which carries a row for every
    selection of every priced match whether or not it was bet.
    """
    by_match = {}
    for row in fixtures:
        if row['book'] != book:
            continue
        key = (row['date'], row['home_team'], row['away_team'])
        by_match.setdefault(key, {'season': row['season'], 'ftr': row['ftr'],
                                  'legs': {}})['legs'][row['selection']] = row
    out = []
    for key in sorted(by_match):
        entry = by_match[key]
        if set(entry['legs']) != set(OUTCOMES):
            raise ValueError(f'incomplete 1x2 for {key}')
        features = []
        for selection in OUTCOMES:
            leg = entry['legs'][selection]
            if leg['market_fair'] <= 0:
                raise ValueError(f'no market probability for {key}')
            features.append((math.log(leg['market_fair']),
                             math.log(max(leg['model_prob'],
                                          MODEL_PROB_FLOOR))))
        out.append((entry['season'], features, RESULT[entry['ftr']]))
    return out


def _solve(matrix, vector):
    """Solve a small dense linear system by Gauss-Jordan elimination."""
    n = len(vector)
    aug = [list(matrix[i]) + [vector[i]] for i in range(n)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(aug[r][col]))
        if abs(aug[pivot][col]) < 1e-12:
            raise ValueError('singular information matrix')
        aug[col], aug[pivot] = aug[pivot], aug[col]
        for r in range(n):
            if r != col:
                factor = aug[r][col] / aug[col][col]
                aug[r] = [x - factor * y for x, y in zip(aug[r], aug[col])]
    return [aug[i][n] / aug[i][i] for i in range(n)]


def _probabilities(features, beta, columns):
    scores = [sum(beta[i] * leg[c] for i, c in enumerate(columns))
              for leg in features]
    top = max(scores)
    weights = [math.exp(s - top) for s in scores]
    total = sum(weights)
    return [w / total for w in weights]


def log_likelihood(obs, beta, columns):
    return sum(math.log(_probabilities(f, beta, columns)[y])
               for _season, f, y in obs)


def fit(obs, columns, start=None, iterations=50, tolerance=1e-10):
    """
    Maximum-likelihood conditional logit on the chosen feature columns
    (0 = ln market, 1 = ln model). Newton-Raphson.

    Returns {'beta', 'se', 'loglik', 'n'}.
    """
    k = len(columns)
    beta = list(start) if start else [1.0] + [0.0] * (k - 1)
    information = None
    for _ in range(iterations):
        gradient = [0.0] * k
        information = [[0.0] * k for _ in range(k)]
        for _season, features, y in obs:
            probs = _probabilities(features, beta, columns)
            mean = [sum(p * leg[c] for p, leg in zip(probs, features))
                    for c in columns]
            for i, ci in enumerate(columns):
                gradient[i] += features[y][ci] - mean[i]
                for j, cj in enumerate(columns):
                    information[i][j] += sum(
                        p * leg[ci] * leg[cj]
                        for p, leg in zip(probs, features)) - mean[i] * mean[j]
        step = _solve(information, gradient)
        beta = [b + s for b, s in zip(beta, step)]
        if max(abs(s) for s in step) < tolerance:
            break
    else:
        raise RuntimeError('conditional logit did not converge')

    se = []
    for i in range(k):
        unit = [1.0 if j == i else 0.0 for j in range(k)]
        se.append(math.sqrt(_solve(information, unit)[i]))
    return {'beta': beta, 'se': se, 'n': len(obs),
            'loglik': log_likelihood(obs, beta, columns)}


def analyse(obs):
    """Every number the diagnostic reports, for one set of matches."""
    n = len(obs)
    market_only = fit(obs, (0,))
    both = fit(obs, (0, 1))
    b, b_se = both['beta'][1], both['se'][1]

    train = [o for o in obs if o[0] in TRAIN_SEASONS]
    test = [o for o in obs if o[0] in TEST_SEASONS]
    oos = None
    if train and test:
        fit_market = fit(train, (0,))
        fit_both = fit(train, (0, 1))
        loss_market = -log_likelihood(test, fit_market['beta'], (0,)) / len(test)
        loss_both = -log_likelihood(test, fit_both['beta'], (0, 1)) / len(test)
        oos = {'n_train': len(train), 'n_test': len(test),
               'market_only': loss_market, 'market_plus_model': loss_both,
               'improvement': loss_market - loss_both}

    return {
        'n': n,
        'logloss_uniform': math.log(3),
        'logloss_market_raw': -log_likelihood(obs, [1.0], (0,)) / n,
        'logloss_model_raw': -log_likelihood(obs, [1.0], (1,)) / n,
        'a_market_only': market_only['beta'][0],
        'a': both['beta'][0], 'a_se': both['se'][0],
        'b': b, 'b_se': b_se,
        'b_ci': (b - 1.96 * b_se, b + 1.96 * b_se),
        'lr_stat': 2 * (both['loglik'] - market_only['loglik']),
        'oos': oos,
    }


def verdict(result):
    return 'SIGNAL' if result['b_ci'][0] > 0 else 'NO SIGNAL'


def _print(label, result):
    low, high = result['b_ci']
    print(f'{label}: {result["n"]:,} matches')
    print(f'  log loss  uniform {result["logloss_uniform"]:.4f} | '
          f'market {result["logloss_market_raw"]:.4f} | '
          f'model {result["logloss_model_raw"]:.4f}')
    print(f'  market only: a = {result["a_market_only"]:.3f}')
    print(f'  market + model: a = {result["a"]:.3f} (se {result["a_se"]:.3f}), '
          f'b = {result["b"]:.3f} (se {result["b_se"]:.3f}), '
          f'95% CI on b {low:.3f} to {high:.3f}, LR stat {result["lr_stat"]:.2f}')
    oos = result['oos']
    if oos:
        print(f'  out of sample ({oos["n_train"]:,} fit, {oos["n_test"]:,} '
              f'scored): log loss market only {oos["market_only"]:.4f}, '
              f'market + model {oos["market_plus_model"]:.4f}, '
              f'improvement {oos["improvement"]:+.4f}')
    print(f'  b reading: {verdict(result)}')
    print()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--league', nargs='+', choices=LEAGUES, required=True)
    parser.add_argument('--odds', choices=sorted(ODDS_FAMILIES), default='avg')
    parser.add_argument('--pool', action='store_true',
                        help='also fit all the --league codes together')
    args = parser.parse_args(argv)

    labels = [season_label(y) for y in BET_YEARS]
    print(f'Odds: {args.odds} | seasons {labels[0]} .. {labels[-1]} | '
          f'model prob floor {MODEL_PROB_FLOOR} | '
          f'P(k) ~ exp(a ln market + b ln model)')
    print()

    pooled = []
    for code in args.league:
        matches, _skips = load_league(code, bet_seasons=BET_YEARS,
                                      families=(args.odds,))
        result = run(matches, seeding='league', league=code,
                     directions=entrant_directions(code),
                     bet_seasons=labels, books=(args.odds,))
        obs = observations(result['fixtures'], args.odds)
        pooled += obs
        _print(code, analyse(obs))
    if args.pool and len(args.league) > 1:
        _print('POOLED ' + '+'.join(args.league), analyse(pooled))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
