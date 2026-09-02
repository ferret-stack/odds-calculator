#!/usr/bin/env python3
"""
Grid search over the shrinkage weight w, scored on two independent metrics.

    python3 backtest/shrinkage.py
    python3 backtest/shrinkage.py --step 0.02
    python3 backtest/shrinkage.py --bands frozen      # lookahead comparison

THE BLEND
---------
At each fixture the probability handed to `pipeline.staking.size_bet` becomes

    p_w = w * model_prob + (1 - w) * market_fair

with w = 1 the production model and w = 0 the de-vigged market. The blend
happens inside the walk-forward loop (`engine.run(shrinkage=w)`), at the one
point where the model probability is used, so there is no second pricing path
that could drift from the first.

TWO METRICS, DELIBERATELY NOT COMBINED
--------------------------------------
1. CALIBRATION -- Brier score, on a FIXED population.
2. YIELD / flat ROI -- production staking, on the bets each w selects.

They are reported as two separate curves and never summed. A single combined
score would need a weighting between "is the number true" and "did it make
money", and that weighting is the operator's judgement, not this script's.

WHY BRIER RATHER THAN LOG LOSS
-------------------------------
Log loss is undefined -- infinite -- at p = 0 on an outcome that happens, and
this model emits exactly 0.0 as a matter of course: `calculate_elo_bands`
produces bands whose weaker_win_pct or draw_pct is 0.0 whenever no match in
the band went that way (band 9 and band 10 of the shipped table both do; see
NOTES.md section 3). A walk-forward rebuilding bands from small early samples
hits those rows constantly. Scoring that with log loss means either an
infinity or an arbitrary epsilon clip, and the clip's value would then set the
ranking. Brier is bounded on [0, 1], needs no clip, and reuses the two fields
every fixture row already carries (`model_prob`, `won`) -- least new
scaffolding, and no free parameter smuggled into the metric.

WHY THE CALIBRATION POPULATION IS FIXED AND THE YIELD POPULATION IS NOT
-----------------------------------------------------------------------
Calibration is scored over EVERY priced selection -- all three legs of all
1,900 post-warm-up fixtures, per book -- not over the bets a given w happened
to select. That population does not depend on w (the ELO chain, the band
tables and the seeding are all independent of any staking decision, which
`_assert_population_is_stable` checks rather than assumes), so the Brier
column is comparable down the whole grid.

Scored on selected bets only it would not be. A w that selects nine bets can
post a flattering Brier on those nine; a w that selects thirteen hundred
cannot. `brier_on_bets` is reported too, because it answers a different and
also useful question, but it is NOT comparable across rows and is labelled as
such wherever it appears.

The yield column has no such option: yield only exists for bets, so its
population necessarily moves with w. That is why `bets` is printed next to it
on every row. Read them together or not at all.
"""

import argparse
import csv
import json
import statistics
import sys
from collections import OrderedDict, defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backtest import report                       # noqa: E402
from backtest.data import DEFAULT_DIR, load_all   # noqa: E402
from backtest.engine import run                   # noqa: E402

OUT_DIR = Path(__file__).resolve().parent / 'out'

# Below this many bets a yield figure is noise dressed as a result. Not a
# filter -- every w is still reported -- but the flag stops a three-bet
# +180% row being read as the best w on the grid.
THIN_SAMPLE = 100

# EV floors for the control sweep. Chosen to span the bet counts the w grid
# produces at its interesting end, so a control row can be matched to a grid
# row by sample size. Production's own floor, +5%, is the first entry.
CONTROL_FLOORS = (0.05, 0.075, 0.10, 0.125, 0.15, 0.175, 0.20, 0.25, 0.30)


def brier(rows, prob_key='model_prob'):
    """
    Mean squared error between a probability and the 0/1 outcome.

    Each fixture contributes its three mutually exclusive 1x2 legs, so this is
    the multiclass Brier score divided by three -- a constant factor, so it
    does not move the minimum, and dividing by the row count keeps the number
    on the familiar [0, 1] scale where lower is better.
    """
    if not rows:
        return None
    total = sum((row[prob_key] - (1.0 if row['won'] else 0.0)) ** 2
                for row in rows)
    return total / len(rows)


def analytic_best_w(rows):
    """
    The exact Brier-minimising w, so the grid's step cannot hide the optimum.

    Brier(w) is a quadratic in w -- writing m for the model probability, f for
    the de-vigged market one and y for the outcome,

        Brier(w) = mean[ ((f - y) + w(m - f))^2 ]

    which is minimised at

        w* = -mean[(f - y)(m - f)] / mean[(m - f)^2]

    UNCONSTRAINED: w* can land outside [0, 1], and if it does that is the
    finding, not an error. w* < 0 means the model's deviation from the market
    points the wrong way on average, so the best available move is past the
    market in the opposite direction -- which the [0, 1] grid cannot express
    and should not pretend to.
    """
    numerator = denominator = 0.0
    for row in rows:
        outcome = 1.0 if row['won'] else 0.0
        spread = row['raw_model_prob'] - row['market_fair']
        numerator += (row['market_fair'] - outcome) * spread
        denominator += spread ** 2
    if not denominator:
        return None
    return -numerator / denominator


def flat_roi_standard_error(rows):
    """
    Standard error of the flat-stake ROI, in percentage points.

    The point of the number is to say whether two w values differ by more than
    the sample can resolve. Flat ROI is a plain mean of per-bet returns
    ((odds - 1) on a win, -1 on a loss), so its SE is sd/sqrt(n) -- whereas
    Kelly yield is a ratio of two random sums and has no one-line SE. That is
    why the interval is quoted on flat ROI and the yield column is left
    without one.

    NAIVE in one respect, stated rather than buried: it assumes bets are
    independent. `check_same_market_conflicts` already blocks two selections
    in one fixture's 1x2, so within-fixture correlation is largely gone, but
    same-day and same-team bets remain correlated. Treat it as a lower bound
    on the true spread -- if a difference is inside this interval it is
    certainly noise; being outside it is not proof it is not.
    """
    if len(rows) < 2:
        return None
    returns = [(r['odds'] - 1.0) if r['won'] else -1.0 for r in rows]
    return statistics.stdev(returns) / (len(returns) ** 0.5) * 100


def _assert_population_is_stable(reference, rows, w):
    """
    Every w must price exactly the same selections, or the Brier column is
    comparing different populations while claiming not to.

    The ELO chain, the band tables and the promoted-team seeding read no
    staking decision, so this must hold. Checked on every grid point anyway --
    the harness's habit is that an invariant it relies on is asserted, not
    assumed.
    """
    key = (lambda r: (r['date'], r['home_team'], r['away_team'], r['book'],
                      r['selection'], r['raw_model_prob'], r['odds']))
    got = {key(r) for r in rows}
    if got != reference:
        raise RuntimeError(
            f'w={w} priced a different set of selections than the first '
            f'grid point ({len(got)} vs {len(reference)} rows, '
            f'{len(got ^ reference)} differing). The band chain is supposed '
            f'to be independent of the staking probability; it is not.')


def grid_points(step):
    """Inclusive 0.0 .. 1.0, rounded so 0.15 is 0.15 and not 0.1500000002."""
    if not 0 < step <= 1:
        raise ValueError(f'step must be in (0, 1], got {step}')
    count = int(round(1.0 / step))
    return [round(i * step, 10) for i in range(count + 1)]


def score(rows, w):
    """One grid row for one book: calibration first, then the money."""
    placed = [r for r in rows if r['bet']]
    summary = report.summarise(placed, f'w={w:.2f}')

    return OrderedDict([
        ('w', round(w, 4)),
        # --- metric 1: calibration, fixed population -----------------------
        ('n_selections', len(rows)),
        ('brier', round(brier(rows), 6)),
        # --- metric 2: yield, population moves with w ----------------------
        ('bets', summary['bets']),
        ('staked', summary['staked']),
        ('profit', summary['profit']),
        ('yield_pct', summary['yield_pct']),
        ('flat_roi_pct', summary['flat_roi_pct']),
        ('flat_roi_se_pct', (round(se, 2)
                             if (se := flat_roi_standard_error(placed))
                             is not None else None)),
        ('strike_rate_pct', summary['strike_rate_pct']),
        ('mean_staked_prob', summary['mean_model_prob']),
        ('mean_market_fair', summary['mean_market_fair']),
        ('mean_edge_pct', summary['mean_edge_pct']),
        ('mean_odds', summary['mean_odds']),
        # Same score as `brier`, but over the bets this w selected. NOT
        # comparable across rows -- the population changes with w.
        ('brier_on_bets', round(brier(placed), 6) if placed else None),
        ('thin_sample', summary['bets'] < THIN_SAMPLE),
    ])


def search(matches, weights, **run_kwargs):
    """
    Run the walk-forward once per w and score both metrics.

    The chain is replayed in full for each w rather than priced once and
    re-staked from the log. It is the more expensive option by a factor of the
    grid size (about 1.8s a run here, so under a minute for 21 points) and it
    is the right one: re-staking from a log means reconstructing the
    per-fixture grouping that `apply_sanity_checks` and `apply_stake_cap` need,
    which is a second implementation of the thing being measured. Replaying
    puts every w through the identical code path.
    """
    grid = defaultdict(list)
    seasons = defaultdict(list)
    reference = None
    endpoint_rows = {}
    meta = None

    for w in weights:
        result = run(matches, shrinkage=w, **run_kwargs)
        rows = result['fixtures']
        meta = result['meta']

        if reference is None:
            reference = {(r['date'], r['home_team'], r['away_team'], r['book'],
                          r['selection'], r['raw_model_prob'], r['odds'])
                         for r in rows}
        else:
            _assert_population_is_stable(reference, rows, w)

        for book in result['meta']['books']:
            book_rows = [r for r in rows if r['book'] == book]
            grid[book].append(score(book_rows, w))
            # Kept for `analytic_best_w`, which reads only raw_model_prob,
            # market_fair and won -- all w-invariant, as the assertion above
            # establishes -- so whichever w gets here first will do.
            endpoint_rows.setdefault(book, book_rows)

            placed = [r for r in book_rows if r['bet']]
            for season in sorted({r['season'] for r in book_rows}):
                cut = [r for r in placed if r['season'] == season]
                summary = report.summarise(cut, season)
                seasons[book].append(OrderedDict([
                    ('w', round(w, 4)), ('season', season),
                    ('bets', summary['bets']),
                    ('staked', summary['staked']),
                    ('profit', summary['profit']),
                    ('yield_pct', summary['yield_pct']),
                    ('flat_roi_pct', summary['flat_roi_pct']),
                ]))

        print(f'  w={w:.2f}  '
              + '  '.join(
                  f"[{b}] brier {g[-1]['brier']:.5f} · "
                  f"{g[-1]['bets']:,} bets · yield "
                  + (f"{g[-1]['yield_pct']:+.2f}%"
                     if g[-1]['yield_pct'] is not None else 'n/a')
                  for b, g in grid.items()))

    return grid, seasons, endpoint_rows, meta


# Columns `report._fmt` would render wrongly: it rounds every float to 2dp,
# which flattens a Brier score that only moves in the fourth decimal, and it
# prints a bool as 1/0 because bool subclasses int. Formatted to strings for
# the markdown only -- the CSV keeps the numbers.
DISPLAY_DP = {'brier': 6, 'brier_on_bets': 6, 'mean_staked_prob': 4,
              'mean_market_fair': 4}


def _display(rows):
    out = []
    for row in rows:
        shown = OrderedDict()
        for key, value in row.items():
            if key == 'thin_sample':
                shown[key] = 'THIN' if value else ''
            elif key in DISPLAY_DP and value is not None:
                shown[key] = f'{value:.{DISPLAY_DP[key]}f}'
            else:
                shown[key] = value
        out.append(shown)
    return out


def season_pivot(rows, value_key='yield_pct'):
    """
    The by-season table with seasons as columns, one row per w.

    Twenty-one weights times five seasons is 105 rows in long form, which
    nobody reads. Pivoted, a w whose result rests on one season is visible at
    a glance -- which is the only reason the table is here.
    """
    seasons = sorted({r['season'] for r in rows})
    by_w = defaultdict(dict)
    for row in rows:
        by_w[row['w']][row['season']] = row

    out = []
    for w in sorted(by_w):
        cells = by_w[w]
        shown = OrderedDict([('w', f'{w:.2f}')])
        for season in seasons:
            cell = cells.get(season)
            value = cell.get(value_key) if cell else None
            shown[season] = ('-' if value is None
                             else f"{value:+.2f}% ({cell['bets']:,})")
        out.append(shown)
    return out


def ev_floor_control(matches, floors, **run_kwargs):
    """
    The control the yield curve needs: raise the EV floor, do not shrink.

    Lowering w shrinks every edge toward the de-vigged price, so fewer
    selections clear the +5% floor and the survivors are the ones where the
    model disagreed with the market most. That is a filter on edge size. An
    EV floor is also a filter on edge size. So before any w can be called a
    calibration improvement, it has to beat the one-line change that selects a
    similar number of bets by simply asking for a bigger edge.

    Run at w=1.0 throughout -- the unshrunk model, floor varied. Compare a row
    here with the grid row that has a similar `bets` count, not with the one
    that has a similar w.
    """
    out = defaultdict(list)
    for floor in floors:
        kwargs = dict(run_kwargs)
        kwargs['min_ev'] = floor
        result = run(matches, shrinkage=1.0, **kwargs)
        for book in result['meta']['books']:
            placed = [r for r in result['fixtures']
                      if r['bet'] and r['book'] == book]
            summary = report.summarise(placed, f'floor={floor:.3f}')
            out[book].append(OrderedDict([
                ('min_ev_pct', round(floor * 100, 2)),
                ('bets', summary['bets']),
                ('staked', summary['staked']),
                ('profit', summary['profit']),
                ('yield_pct', summary['yield_pct']),
                ('flat_roi_pct', summary['flat_roi_pct']),
                ('strike_rate_pct', summary['strike_rate_pct']),
                ('mean_edge_pct', summary['mean_edge_pct']),
            ]))
        print(f'  floor={floor * 100:5.2f}%  '
              + '  '.join(f"[{b}] {r[-1]['bets']:,} bets · yield "
                          + (f"{r[-1]['yield_pct']:+.2f}%"
                             if r[-1]['yield_pct'] is not None else 'n/a')
                          for b, r in out.items()))
    return out


def _best(rows, key, want_max):
    """The grid row optimising `key`, ignoring rows where it is undefined."""
    live = [r for r in rows if r[key] is not None]
    if not live:
        return None
    return (max if want_max else min)(live, key=lambda r: r[key])


def render_markdown(grid, seasons, control, analytic, meta, thin):
    lines = [
        '# Shrinkage grid search — blending the model toward the market',
        '',
        f"- **Blend**: `p_w = w * model_prob + (1 - w) * market_fair`, "
        f"applied inside the walk-forward at the point `size_bet` is called",
        f"- **Bands**: `{meta['bands_mode']}` · "
        f"**warm-up (no bets)**: {', '.join(meta['warmup_seasons']) or 'none'}"
        f" · {meta['priced_matches']:,} matches priced",
        f"- **Staking**: unchanged production `pipeline.staking` — "
        f"+{meta['min_ev'] * 100:.0f}% EV floor, Quarter-Kelly ceiling, "
        f"Eighth-Kelly at or above +20%, "
        f"{meta['max_stake_fraction'] * 100:.0f}% per-bet cap",
        f"- **Calibration**: Brier score over every priced selection "
        f"(population fixed across w). Lower is better.",
        f"- **Yield**: production staking on the bets each w selects "
        f"(population MOVES with w — read `bets` alongside).",
        '',
        f'`w = 1.00` is the production model as it stands; `w = 0.00` is the '
        f'de-vigged market with no model input. Rows with fewer than '
        f'{thin:,} bets are flagged `thin_sample` — a yield on that few bets '
        f'is not a measurement.',
        '',
    ]

    for book, rows in grid.items():
        title = ('AvgH/AvgD/AvgA — market-average closing odds (primary)'
                 if book == 'avg'
                 else 'B365H/B365D/B365A — single bookmaker (secondary)')
        best_brier = _best(rows, 'brier', want_max=False)
        best_yield = _best(rows, 'yield_pct', want_max=True)
        best_flat = _best(rows, 'flat_roi_pct', want_max=True)
        fat = [r for r in rows if not r['thin_sample']]
        best_fat_yield = _best(fat, 'yield_pct', want_max=True)

        lines += [f'## {title}', '']
        lines += [
            f"- **Minimum Brier** at "
            f"`w = {best_brier['w']:.2f}` ({best_brier['brier']:.6f}); "
            f"exact unconstrained minimiser "
            f"`w* = {analytic[book]:.4f}`"
            if analytic.get(book) is not None else
            f"- **Minimum Brier** at `w = {best_brier['w']:.2f}`",
            f"- **Maximum yield** at `w = {best_yield['w']:.2f}` "
            f"({best_yield['yield_pct']:+.2f}% on {best_yield['bets']:,} bets)"
            + ('  ⚠ **thin sample**' if best_yield['thin_sample'] else ''),
            f"- **Maximum flat ROI** at `w = {best_flat['w']:.2f}` "
            f"({best_flat['flat_roi_pct']:+.2f}% on {best_flat['bets']:,} bets)"
            + ('  ⚠ **thin sample**' if best_flat['thin_sample'] else ''),
        ]
        if best_fat_yield is not None:
            lines.append(
                f"- **Maximum yield on ≥{thin:,} bets** at "
                f"`w = {best_fat_yield['w']:.2f}` "
                f"({best_fat_yield['yield_pct']:+.2f}% on "
                f"{best_fat_yield['bets']:,} bets)")
        # Whether the yield peak is bigger than the sample can resolve. Stated
        # here rather than left to the reader, because a peak inside one
        # standard error is the difference between a parameter and a wiggle.
        production = next(r for r in rows if abs(r['w'] - 1.0) < 1e-9)
        if (best_fat_yield is not None
                and best_fat_yield['flat_roi_se_pct'] is not None
                and production['flat_roi_se_pct'] is not None):
            gap = best_fat_yield['flat_roi_pct'] - production['flat_roi_pct']
            spread = (best_fat_yield['flat_roi_se_pct'] ** 2
                      + production['flat_roi_se_pct'] ** 2) ** 0.5
            lines.append(
                f"- **Is the peak real?** flat ROI at that w beats `w = 1.00` "
                f"by {gap:+.2f}pp, against a combined standard error of "
                f"±{spread:.2f}pp "
                + ('— **inside the noise**' if abs(gap) < spread
                   else '— outside one standard error, though the two w '
                        'values share most of their bets, so the difference '
                        'is far less independent than that comparison '
                        'suggests'))
        lines += ['', '### Both curves, per w', '',
                  '_`brier` is metric 1 on a population fixed across every '
                  'row. Everything from `bets` rightward is metric 2, on a '
                  'population that changes with w. `flat_roi_se_pct` is one '
                  'naive standard error on `flat_roi_pct`._', '',
                  report.markdown_table(_display(rows)), '']

        lines += ['### Yield by season, per w — cell is `yield% (bets)`', '',
                  '_The grid picks w on the same data it scores it on. A w '
                  'whose yield rests on one season is an artefact of that '
                  'season, not a parameter._', '',
                  report.markdown_table(season_pivot(seasons[book])), '']

        lines += ['### Control: the unshrunk model at a raised EV floor', '',
                  '_Shrinking w and raising the floor are both filters on edge '
                  'size. Match a control row to the grid row with a similar '
                  '`bets` count — not a similar w — and ask whether shrinkage '
                  'bought anything the floor did not._', '',
                  report.markdown_table(control[book]), '']

    return '\n'.join(lines)


def write_csv(path, rows):
    if not rows:
        path.write_text('')
        return
    fieldnames = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with open(path, 'w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--data', default=str(DEFAULT_DIR))
    parser.add_argument('--out', default=str(OUT_DIR))
    parser.add_argument('--step', type=float, default=0.05,
                        help='grid step for w (default 0.05)')
    parser.add_argument('--bands', choices=('walkforward', 'frozen'),
                        default='walkforward')
    parser.add_argument('--warmup-seasons', type=int, default=1)
    parser.add_argument('--min-ev', type=float, default=None,
                        help='EV floor as a fraction; defaults to staking.MIN_EV')
    args = parser.parse_args(argv)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    matches = load_all(args.data)
    weights = grid_points(args.step)
    print(f'Loaded {len(matches):,} matches from {args.data}')
    print(f'Grid: {len(weights)} weights from '
          f'{weights[0]:.2f} to {weights[-1]:.2f} step {args.step}')

    run_kwargs = {'bands_mode': args.bands,
                  'warmup_seasons': args.warmup_seasons}
    if args.bands == 'frozen':
        frozen_path = REPO_ROOT / 'data' / 'elo_bands.json'
        run_kwargs['frozen_bands'] = json.loads(frozen_path.read_text())
        print(f'Using frozen production bands from {frozen_path} '
              f'(LOOKAHEAD — comparison only)')
    if args.min_ev is not None:
        run_kwargs['min_ev'] = args.min_ev

    grid, seasons, endpoint_rows, meta = search(matches, weights, **run_kwargs)

    print('\nControl — unshrunk model (w=1.0) at a raised EV floor:')
    control = ev_floor_control(matches, CONTROL_FLOORS, **run_kwargs)

    # The exact minimiser, from the same fixed population the grid scores.
    analytic = {book: analytic_best_w(rows)
                for book, rows in endpoint_rows.items()}

    write_csv(out_dir / 'shrinkage_ev_floor_control.csv',
              [{'book': book, **row} for book, rows in control.items()
               for row in rows])
    write_csv(out_dir / 'shrinkage_grid.csv',
              [{'book': book, **row} for book, rows in grid.items()
               for row in rows])
    write_csv(out_dir / 'shrinkage_by_season.csv',
              [{'book': book, **row} for book, rows in seasons.items()
               for row in rows])
    (out_dir / 'shrinkage.md').write_text(
        render_markdown(grid, seasons, control, analytic, meta, THIN_SAMPLE),
        encoding='utf-8')

    print(f'\nWrote shrinkage_grid.csv, shrinkage_by_season.csv, '
          f'shrinkage_ev_floor_control.csv, shrinkage.md to {out_dir}/')

    for book, rows in grid.items():
        best_brier = _best(rows, 'brier', want_max=False)
        best_yield = _best(rows, 'yield_pct', want_max=True)
        exact = analytic.get(book)
        print(f'\n[{book}] min Brier   w={best_brier["w"]:.2f} '
              f'({best_brier["brier"]:.6f})'
              + (f'  exact w*={exact:.4f}' if exact is not None else ''))
        print(f'[{book}] max yield   w={best_yield["w"]:.2f} '
              f'({best_yield["yield_pct"]:+.2f}% on {best_yield["bets"]:,} '
              f'bets)'
              + ('  [THIN SAMPLE]' if best_yield['thin_sample'] else ''))
        # The control row that selects a comparable number of bets, so the
        # yield peak is never read without the thing it has to beat.
        nearest = min(control[book],
                      key=lambda r: abs(r['bets'] - best_yield['bets']))
        print(f'[{book}] control     unshrunk model at a '
              f'{nearest["min_ev_pct"]:.1f}% EV floor: '
              f'{nearest["yield_pct"]:+.2f}% on {nearest["bets"]:,} bets')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
