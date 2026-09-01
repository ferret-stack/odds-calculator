"""
Aggregate the fixture log into the ROI/yield breakdowns.

TWO RETURN MEASURES, AND WHY BOTH
---------------------------------
`yield_pct` is profit over money actually staked, at production's own
Quarter-Kelly sizing (including its Eighth-Kelly downgrade above +20% and its
3%-of-bankroll per-bet clamp). It answers "what did this strategy return".

`flat_roi_pct` is profit over a flat one-unit stake on the same selections.
It answers "was the SELECTION any good", independent of how Kelly happened to
size it. The two diverge exactly where Kelly's sizing is doing work, so a
bucket where they disagree is a bucket where the sizing rule -- not the edge
-- is driving the result. Reporting only the first would hide that.

Calibration is reported beside both, because it is the measure that actually
answers the question the brief asks. A model can show a positive edge on every
fixture and still lose: `mean_model_prob` versus `strike_rate` says whether
the probabilities were true, and `mean_market_fair` says whether the market's
de-vigged view was closer.
"""

from collections import OrderedDict, defaultdict

EDGE_BUCKETS = (
    ('5-10%', 5.0, 10.0),
    ('10-20%', 10.0, 20.0),
    ('20%+', 20.0, float('inf')),
)


def edge_bucket(edge_pct):
    for label, low, high in EDGE_BUCKETS:
        if low <= edge_pct < high:
            return label
    return 'below floor'


def summarise(rows, label):
    """One metrics row for a set of bets. `rows` are placed bets only."""
    bets = len(rows)
    if not bets:
        return OrderedDict([('bucket', label), ('bets', 0), ('staked', 0.0),
                            ('profit', 0.0), ('yield_pct', None),
                            ('flat_roi_pct', None), ('strike_rate_pct', None),
                            ('mean_model_prob', None),
                            ('mean_market_fair', None),
                            ('mean_edge_pct', None), ('mean_odds', None)])

    staked = sum(r['stake'] for r in rows)
    profit = sum(r['profit'] for r in rows)
    wins = sum(1 for r in rows if r['won'])
    # Flat stake of 1 unit per selection.
    flat_profit = sum((r['odds'] - 1) if r['won'] else -1.0 for r in rows)

    def mean(key):
        return sum(r[key] for r in rows) / bets

    return OrderedDict([
        ('bucket', label),
        ('bets', bets),
        ('staked', round(staked, 2)),
        ('profit', round(profit, 2)),
        ('yield_pct', round(profit / staked * 100, 2) if staked else None),
        ('flat_roi_pct', round(flat_profit / bets * 100, 2)),
        ('strike_rate_pct', round(wins / bets * 100, 2)),
        ('mean_model_prob', round(mean('model_prob'), 4)),
        ('mean_market_fair', round(mean('market_fair'), 4)),
        ('mean_edge_pct', round(mean('edge_pct'), 2)),
        ('mean_odds', round(mean('odds'), 2)),
    ])


def group(rows, key, label_fn=str, order=None):
    """Summarise `rows` grouped by `key`, in `order` if given."""
    buckets = defaultdict(list)
    for row in rows:
        buckets[key(row)].append(row)
    keys = order if order is not None else sorted(buckets)
    return [summarise(buckets.get(k, []), label_fn(k)) for k in keys
            if order is None or k in buckets or True]


def bankroll_curve(rows, start=1000.0):
    """
    Compounded bankroll, replaying the same stake FRACTIONS in date order.

    Kelly fractions are bankroll-independent, and so is the 3% clamp, so the
    fractions computed against the notional flat bankroll are the right ones
    to compound. Bets are settled in date order; same-day bets are settled
    together against the bankroll they started the day with, which is what
    placing a matchday's slate actually does.
    """
    bankroll, peak, max_dd = start, start, 0.0
    by_date = defaultdict(list)
    for row in rows:
        by_date[row['date']].append(row)

    for date in sorted(by_date):
        opening = bankroll
        for row in by_date[date]:
            stake = opening * row['stake_fraction']
            bankroll += stake * (row['odds'] - 1) if row['won'] else -stake
            if bankroll <= 0:
                return {'final': 0.0, 'max_drawdown_pct': 100.0,
                        'ruin_date': date, 'start': start}
        peak = max(peak, bankroll)
        max_dd = max(max_dd, (peak - bankroll) / peak * 100)

    return {'final': round(bankroll, 2),
            'max_drawdown_pct': round(max_dd, 2),
            'ruin_date': None, 'start': start}


def calibration(rows, edges=(0.0, 0.2, 0.3, 0.4, 0.5, 0.6, 1.01)):
    """Model probability bucket vs. what actually happened."""
    out = []
    for low, high in zip(edges, edges[1:]):
        band = [r for r in rows if low <= r['model_prob'] < high]
        if not band:
            continue
        out.append(OrderedDict([
            ('model_prob_range', f'{low:.2f}-{high:.2f}'),
            ('bets', len(band)),
            ('mean_model_prob', round(sum(r['model_prob'] for r in band) / len(band), 4)),
            ('actual_win_rate', round(sum(1 for r in band if r['won']) / len(band), 4)),
            ('mean_market_fair', round(sum(r['market_fair'] for r in band) / len(band), 4)),
        ]))
    return out


def build(result, book):
    """Every breakdown the brief asks for, for one odds source."""
    placed = [r for r in result['fixtures'] if r['bet'] and r['book'] == book]
    considered = [r for r in result['fixtures'] if r['book'] == book]

    bands_seen = sorted({r['band'] for r in placed})
    seasons = sorted({r['season'] for r in placed})

    # The brief's Theme 2 cut. `implausible` is edge >= 20% and nothing else
    # (see NOTES.md); `away_underdog` is the away selection with the home
    # side ELO-stronger. Reported as the 2x2 so the two are not conflated.
    def quadrant(row):
        return ('away-underdog' if row['away_underdog'] else 'other selection',
                'edge >=20%' if row['implausible'] else 'edge <20%')

    away_dog_implausible = [r for r in placed
                            if r['away_underdog'] and r['implausible']]

    return OrderedDict([
        ('overall', [summarise(placed, 'all bets')]),
        ('by_selection', group(placed, lambda r: r['selection'],
                               order=['home', 'draw', 'away'])),
        ('by_band', group(placed, lambda r: r['band'],
                          label_fn=lambda b: f'band {b}', order=bands_seen)),
        ('by_edge_bucket', group(placed, lambda r: edge_bucket(r['edge_pct']),
                                 order=[b[0] for b in EDGE_BUCKETS])),
        ('implausible_edge_pattern',
         group(placed, quadrant, label_fn=lambda k: f'{k[0]}, {k[1]}')),
        ('away_underdog_implausible',
         [summarise(away_dog_implausible,
                    'away underdog with edge >=20%')]),
        ('by_season', group(placed, lambda r: r['season'], order=seasons)),
        ('calibration', calibration(placed)),
        ('bankroll', bankroll_curve(placed)),
        ('counts', {
            'selections_considered': len(considered),
            'bets_placed': len(placed),
            'blocked_by_sanity_check': len(
                [r for r in considered
                 if not r['bet'] and 'same_market_multiple_ev' in r['flags']]),
            'per_bet_cap_fired': len([r for r in placed if 'per_bet_cap' in r['flags']]),
            'eighth_kelly_large_edge': len(
                [r for r in placed if r['confidence'] == 'large_edge']),
        }),
    ])


# --- rendering ------------------------------------------------------------

def _fmt(value):
    if value is None:
        return '-'
    if isinstance(value, float):
        return f'{value:,.2f}'
    if isinstance(value, int):
        return f'{value:,}'
    return str(value)


def markdown_table(rows):
    if not rows:
        return '_(no rows)_\n'
    headers = list(rows[0].keys())
    out = ['| ' + ' | '.join(headers) + ' |',
           '|' + '|'.join('---' for _ in headers) + '|']
    for row in rows:
        out.append('| ' + ' | '.join(_fmt(row.get(h)) for h in headers) + ' |')
    return '\n'.join(out) + '\n'
