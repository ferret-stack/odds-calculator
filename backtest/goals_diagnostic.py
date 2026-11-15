#!/usr/bin/env python3
"""
Goals diagnostic: does the band table's over-2.5 rate know anything the
over/under 2.5 market price does not?

    python3 backtest/goals_diagnostic.py --league E2 E3 --pool
    python3 backtest/goals_diagnostic.py --league E1 EC E0
    python3 backtest/goals_diagnostic.py --league E2 E3 --odds ou25c --pool

Prints only. Writes no files. Places no bets. The 2025/26 holdout cannot be
reached (same loader, same guard).

DECISIONS, FIXED BEFORE ANY RESULT (2026-11-15)
-----------------------------------------------
What      The model's P(over 2.5) is the walk-forward band table's
          `over_25_pct` for the match's ELO-difference band, as it stood
          before the match date: one rate per band, no venue adjustment, no
          team identity. Production does not bet goals; this tests the band
          table's goals column, not anything production does.
Data      Same chains as the lower-league backtest: per-league ELO, league
          seeding, walk-forward bands, seasons 2019/20-2024/25. A match is
          used if the engine priced it (model exists: band non-empty and not
          degenerate) and both over/under prices exist in the family. Matches
          priced by the model but missing a price are counted and excluded.
Inputs    m = market probability, de-vigged proportionally over the two
          outcomes. p = over_25_pct clamped to [0.01, 0.99] (a small band
          can emit exactly 0 or 1, whose log is undefined).
Model     The two-outcome form of diagnostic.py's conditional logit, reusing
          its fit/analyse/verdict unchanged:

              P(outcome k)  proportional to  exp(a*ln m_k + b*ln p_k)
              i.e. logit P(over) = a*logit(m) + b*logit(p)

          No intercept, so it is exactly the 1x2 beta test with two outcomes.
          a = 1, b = 0 is "the market is right and the model adds nothing".
Reading   PRIMARY: E2 + E3 pooled, ou25 (Avg, pre-closing).
          SIGNAL    = the 95% CI on b lies entirely above zero.
          NO SIGNAL = anything else, including a CI that spans zero.
          E2 and E3 separately, E0, E1, EC, ou25c (AvgC), the out-of-sample
          log-loss difference, and mean p / mean m / observed over rate are
          descriptive only.
Meaning   NO SIGNAL: the band table's goals rate adds nothing to the market;
          goals markets are closed for this model as it stands.
          SIGNAL: a goals betting test is worth its own pre-registration and
          a holdout check. It is NOT evidence of profit.
Secondary Out-of-sample, descriptive: fit on 2019/20-2021/22, score
          2022/23-2024/25, as in diagnostic.py.
"""

import argparse
import math
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backtest.data import (  # noqa: E402
    GOALS_FAMILIES, LEAGUES, entrant_directions, load_league, season_label)
from backtest.diagnostic import _print as _print_fit  # noqa: E402
from backtest.diagnostic import analyse as _analyse_1x2  # noqa: E402
from backtest.diagnostic import verdict  # noqa: E402,F401
from backtest.engine import run  # noqa: E402

BET_YEARS = tuple(range(2020, 2026))          # 2019/20 .. 2024/25
MODEL_PROB_FLOOR = 0.01
MODEL_PROB_CEILING = 0.99
LEGS = ('over', 'under')                      # outcome 0 = over, 1 = under


def observations(goal_rates, matches, family):
    """
    One observation per usable match: (season, [(ln m, ln p) x2], winner).

    goal_rates -- the engine's per-priced-match band rows
    matches    -- the loaded matches, for their prices in `family`
    Returns (obs, summary) where summary has 'missing_price' (priced by the
    model, no usable O/U price) and the means of p, m and the over rate.
    """
    prices = {(m['date'], m['home_team'], m['away_team']):
              m['odds'].get(family) for m in matches}
    obs, missing = [], 0
    sum_p = sum_m = overs = 0.0
    for rate in goal_rates:
        key = (rate['date'], rate['home_team'], rate['away_team'])
        if key not in prices:
            raise ValueError(f'priced match {key} not among the loaded matches')
        quote = prices[key]
        if not quote:
            missing += 1
            continue
        total = 1.0 / quote['over'] + 1.0 / quote['under']
        m_over = (1.0 / quote['over']) / total
        p_over = min(max(rate['over_25_pct'], MODEL_PROB_FLOOR),
                     MODEL_PROB_CEILING)
        over = rate['total_goals'] > 2.5
        features = [(math.log(m_over), math.log(p_over)),
                    (math.log(1.0 - m_over), math.log(1.0 - p_over))]
        obs.append((rate['season'], features, 0 if over else 1))
        sum_p += p_over
        sum_m += m_over
        overs += over
    n = len(obs)
    summary = {'missing_price': missing,
               'mean_p': sum_p / n if n else None,
               'mean_m': sum_m / n if n else None,
               'over_rate': overs / n if n else None}
    return obs, summary


def analyse(obs):
    """diagnostic.analyse, with the uniform baseline for two outcomes."""
    result = _analyse_1x2(obs)
    result['logloss_uniform'] = math.log(len(LEGS))
    return result


def league_obs(code, family, years=BET_YEARS):
    matches, _skips = load_league(code, bet_seasons=years, families=(family,))
    result = run(matches, seeding='league', league=code,
                 directions=entrant_directions(code),
                 bet_seasons=[season_label(y) for y in years], books=())
    return observations(result['goal_rates'], matches, family)


def _print(label, result, summary):
    print(f'{label} data: excluded (no usable O/U price) {summary["missing_price"]} '
          f'| mean model p {summary["mean_p"]:.4f} | mean market m '
          f'{summary["mean_m"]:.4f} | observed over rate '
          f'{summary["over_rate"]:.4f}')
    _print_fit(label, result)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--league', nargs='+', choices=LEAGUES, required=True)
    parser.add_argument('--odds', choices=sorted(GOALS_FAMILIES),
                        default='ou25')
    parser.add_argument('--pool', action='store_true',
                        help='also fit all the --league codes together')
    args = parser.parse_args(argv)

    labels = [season_label(y) for y in BET_YEARS]
    print(f'Odds: {args.odds} (over/under 2.5) | seasons {labels[0]} .. '
          f'{labels[-1]} | model p = band over_25_pct clamped to '
          f'[{MODEL_PROB_FLOOR}, {MODEL_PROB_CEILING}] | '
          f'logit P(over) = a logit(market) + b logit(model)')
    print()

    pooled, pooled_summary = [], []
    for code in args.league:
        obs, summary = league_obs(code, args.odds)
        pooled += obs
        pooled_summary.append((len(obs), summary))
        _print(code, analyse(obs), summary)
    if args.pool and len(args.league) > 1:
        n = sum(k for k, _ in pooled_summary)
        merged = {'missing_price': sum(s['missing_price']
                                       for _, s in pooled_summary)}
        for field in ('mean_p', 'mean_m', 'over_rate'):
            merged[field] = sum(k * s[field] for k, s in pooled_summary) / n
        _print('POOLED ' + '+'.join(args.league), analyse(pooled), merged)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
