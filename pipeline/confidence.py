"""
Sample size and confidence intervals for band-derived probabilities.

INSTRUMENTATION ONLY. Nothing here sizes a bet, and nothing in the staking
path reads it. Using an interval to actually adjust a stake is a separate
decision that has not been taken; this module exists so that decision can be
made against numbers rather than impressions.

WHY THIS EXISTS
---------------
`calculate_fair_odds` prices every fixture from one row of elo_bands.json.
The row for Band 1 rests on 674 matches; the row for Band 10 rests on 4. Both
arrive in the output as a bare probability to four decimal places, and a
probability of 1.0000 from four matches reads exactly like a probability of
1.0000 that means something. The point estimate cannot tell those apart. The
sample size and the interval can.

WHICH INTERVAL
--------------
Wilson score, as asked for, and it is the right default here.

The bands run from n=674 down to n=4, and the thin bands sit at proportions
pinned to the boundary: Band 10 is 4/4 stronger-team wins, Band 9 is 0/6
weaker-team wins. That is the regime where the textbook Wald interval fails
outright -- at p-hat = 0 or 1 its width collapses to zero, so the least
trustworthy row in the file would report the most certain-looking interval.
Wilson does not degenerate there: it stays inside [0, 1], keeps non-zero
width at the boundary, and holds its nominal coverage far better than Wald at
small n. It is also cheap and closed-form, which matters for something
recomputed on every fixture of every run.

Two reasonable alternatives, and why not:

  Clopper-Pearson (exact) guarantees at-least-nominal coverage, but buys that
  guarantee with conservative width. On n=4 it spans almost the whole unit
  interval, which is true but not informative, and it would make the thin
  bands look uniformly hopeless rather than differently thin.

  Jeffreys (Beta(1/2, 1/2) posterior) is arguably the better small-n choice on
  average coverage and would be a defensible swap. It needs an incomplete beta
  function, and it differs from Wilson by little enough at these n that the
  extra dependency is not worth it for an instrumentation column.

The honest caveat, which no interval fixes: these bounds describe SAMPLING
error in the historical band rate only. They say nothing about whether the
band is the right reference class for a given fixture -- which is the failure
mode the implausible-edge flag is actually about. A tight interval on a coarse
band is still a coarse band.
"""

from elo_calculator import adjust_probability_for_venue

# 1.959964 is the exact two-sided 95% normal quantile. Spelled out rather
# than pulled from scipy: this module has no numeric dependencies, and the
# repo has none to inherit.
Z_95 = 1.959963984540054

# model output key -> the band column it is derived from, given which side is
# the ELO-stronger team. The staking code never needs this mapping; the
# interval does, because the interval belongs to the RAW band rate, and the
# raw rates are recorded stronger/draw/weaker rather than home/draw/away.
_MARKETS = ('stronger_win', 'draw', 'weaker_win')


def wilson_interval(successes, n, z=Z_95):
    """
    Wilson score interval for a binomial proportion.

                p~ +- (z / (1 + z^2/n)) * sqrt(p~(1-p~)/n + z^2/(4n^2))
        centre = (p~ + z^2/2n) / (1 + z^2/n)

    Returns (low, high), both clipped to [0, 1]. An empty sample returns
    (0.0, 1.0) -- with no data the honest interval is the whole range, not a
    division by zero.
    """
    if n < 0:
        raise ValueError(f'n must be non-negative, got {n}')
    if not 0 <= successes <= n:
        raise ValueError(f'successes must be in [0, {n}], got {successes}')
    if n == 0:
        return 0.0, 1.0

    p = successes / n
    z2 = z * z
    denominator = 1 + z2 / n
    centre = (p + z2 / (2 * n)) / denominator
    margin = (z / denominator) * ((p * (1 - p) / n
                                   + z2 / (4 * n * n)) ** 0.5)
    return max(0.0, centre - margin), min(1.0, centre + margin)


def find_band(band_number, elo_bands):
    """The elo_bands row for a band number, or None."""
    for band in elo_bands:
        if band['band'] == band_number:
            return band
    return None


def _venue_scale(band, is_stronger_home):
    """
    Per-market factor carrying a RAW band rate to the model's final one.

    calculate_fair_odds multiplies each raw rate by a venue multiplier and
    then normalises the three to sum to 1, so

        final_i = raw_i * m_i / sum_j(raw_j * m_j)

    and the factor is m_i / sum_j(raw_j * m_j) -- independent of raw_i, which
    is why it can be applied to an interval bound as well as to a point, and
    why it is well defined even where a raw rate is 0 (Bands 9 and 10 both
    have one).

    This reproduces the transform rather than altering it: nothing in
    elo_calculator is modified, and `adjust_probability_for_venue` is imported
    and reused so the two cannot drift apart.
    """
    multipliers = {
        market: adjust_probability_for_venue(1.0, is_stronger_home, market)
        for market in _MARKETS
    }
    total = sum(band[f'{market}_pct'] * multipliers[market]
                for market in _MARKETS)
    if total <= 0:
        return {market: 0.0 for market in _MARKETS}
    return {market: multipliers[market] / total for market in _MARKETS}


def band_evidence(model, elo_bands, z=Z_95):
    """
    Sample size and interval behind each 1x2 probability of one priced fixture.

    `model` is the dict returned by calculate_fair_odds. Returns a dict keyed
    by its probability keys -- 'home_win', 'draw', 'away_win' -- each carrying:

        band, band_range, match_count      the row the number came from
        band_proportion                    the RAW historical rate
        observed_matches                   band_proportion x n, rounded
        ci_low / ci_high                   Wilson bounds on the RAW rate
        adjusted_probability               the model's number, for reference
        adjusted_ci_low / adjusted_ci_high the same bounds carried through the
                                           venue adjustment and normalisation,
                                           so they bracket the number actually
                                           reported
        ci_width                           on the adjusted scale
        interval                           'wilson_95' (or the z used)

    Both scales are given deliberately. The raw pair is where the statistics
    actually live; the adjusted pair is the one that can be read against the
    probability in the output table without a mental conversion.

    Returns {} if the band cannot be found -- instrumentation must never be
    the thing that stops a run.
    """
    meta = model.get('meta', {})
    band_number = meta.get('band')
    band = find_band(band_number, elo_bands)
    if band is None:
        return {}

    is_stronger_home = meta.get('stronger_team') == 'home'
    scale = _venue_scale(band, is_stronger_home)
    n = int(band.get('total_games', 0))

    # Which band column feeds which output key, for this fixture's orientation.
    sources = {
        'home_win': 'stronger_win' if is_stronger_home else 'weaker_win',
        'draw': 'draw',
        'away_win': 'weaker_win' if is_stronger_home else 'stronger_win',
    }

    evidence = {}
    for key, market in sources.items():
        proportion = band[f'{market}_pct']
        # The band file stores rates, not counts. Recovering the count by
        # rounding is exact to within the file's own 4dp rounding, and is the
        # only route to a count without re-deriving the bands -- which is out
        # of scope here and would touch banding code.
        successes = int(round(proportion * n))
        low, high = wilson_interval(successes, n, z=z)
        factor = scale[market]

        adjusted_low = min(1.0, low * factor)
        adjusted_high = min(1.0, high * factor)

        evidence[key] = {
            'band': band_number,
            'band_range': band.get('range'),
            'match_count': n,
            'observed_matches': successes,
            'band_proportion': round(proportion, 4),
            'ci_low': round(low, 4),
            'ci_high': round(high, 4),
            'adjusted_probability': model.get(key, {}).get('probability'),
            'adjusted_ci_low': round(adjusted_low, 4),
            'adjusted_ci_high': round(adjusted_high, 4),
            'ci_width': round(adjusted_high - adjusted_low, 4),
            'interval': 'wilson_95' if z == Z_95 else f'wilson_z={z:g}',
        }
    return evidence


def summarise(entry):
    """One-line rendering for the terminal report."""
    if not entry:
        return 'no band evidence'
    return (f"band {entry['band']} ({entry['band_range']}), "
            f"n={entry['match_count']}, "
            f"{entry['interval']} "
            f"[{entry['adjusted_ci_low']:.4f}, {entry['adjusted_ci_high']:.4f}] "
            f"(width {entry['ci_width']:.4f})")
