#!/usr/bin/env python3
"""
Run the historical backtest and write the ROI breakdown.

    python3 backtest/run_backtest.py
    python3 backtest/run_backtest.py --bands frozen      # lookahead comparison
    python3 backtest/run_backtest.py --warmup-seasons 2

Writes to backtest/out/ and nowhere else. Reads docs/historical/*.csv and,
in --bands frozen, data/elo_bands.json. Nothing in data/ or pipeline/ is
written, and bankroll.json is never opened.
"""

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backtest import report  # noqa: E402
from backtest.data import DEFAULT_DIR, load_all  # noqa: E402
from backtest.engine import run  # noqa: E402

OUT_DIR = Path(__file__).resolve().parent / 'out'

SECTIONS = [
    ('overall', 'Overall'),
    ('by_selection', 'By selection (home / draw / away)'),
    ('by_band', 'By ELO band'),
    ('by_edge_bucket', 'By edge size'),
    ('implausible_edge_pattern',
     'Away-underdog x implausible-edge (the Theme 2 cut)'),
    ('away_underdog_implausible', 'Away underdog AND edge >=20%, isolated'),
    ('by_season', 'By season'),
    ('calibration', 'Calibration: model probability vs. what happened'),
]


def write_csv(path, rows, fieldnames=None):
    if not rows:
        path.write_text('')
        return
    fieldnames = fieldnames or list(rows[0].keys())
    with open(path, 'w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def render_markdown(result, summaries):
    meta = result['meta']
    anomalies = Counter(a['kind'] for a in result['anomalies'])
    lines = [
        '# ELO band model vs. market closing odds — historical backtest',
        '',
        f"- **Bands**: `{meta['bands_mode']}`"
        + ('  (rebuilt at each match date from matches completed strictly '
           'before it)' if meta['bands_mode'] == 'walkforward'
           else '  (production `data/elo_bands.json`, built over the whole '
                'sample — **contains lookahead, comparison only**)'),
        f"- **Seasons**: {', '.join(meta['seasons'])} "
        f"({meta['matches']:,} matches)",
        f"- **Warm-up (no bets)**: "
        f"{', '.join(meta['warmup_seasons']) or 'none'} — "
        f"{meta['priced_matches']:,} matches priced",
        f"- **Staking**: production `pipeline.staking` — Quarter-Kelly at the "
        f"+{meta['min_ev'] * 100:.0f}% EV floor, Eighth-Kelly at or above "
        f"+20%, {meta['max_stake_fraction'] * 100:.0f}% per-bet cap, "
        f"notional bankroll {meta['notional_bankroll']:,.0f}",
        f"- **Anomalies logged**: "
        + (', '.join(f'{k} x{v}' for k, v in sorted(anomalies.items()))
           or 'none'),
        '',
        '`yield_pct` = profit / staked at Kelly sizing. `flat_roi_pct` = '
        'profit / bets at a flat 1-unit stake. `market_fair` = de-vigged '
        'market probability.',
        '',
    ]

    for book, summary in summaries.items():
        title = ('AvgH/AvgD/AvgA — market-average closing odds (primary)'
                 if book == 'avg'
                 else 'B365H/B365D/B365A — single bookmaker (secondary)')
        lines += [f'## {title}', '']
        counts = summary['counts']
        bank = summary['bankroll']
        lines += [
            f"Selections considered: {counts['selections_considered']:,} · "
            f"bets placed: {counts['bets_placed']:,} · "
            f"blocked by same-market sanity check: "
            f"{counts['blocked_by_sanity_check']:,} · "
            f"sized down to Eighth-Kelly for a >=20% edge: "
            f"{counts['eighth_kelly_large_edge']:,} · "
            f"per-bet cap fired: {counts['per_bet_cap_fired']:,}",
            '',
            f"Compounded bankroll: {bank['start']:,.0f} -> "
            f"{bank['final']:,.2f}"
            + (f" (RUIN on {bank['ruin_date']})" if bank['ruin_date']
               else f" · max drawdown {bank['max_drawdown_pct']:.2f}%"),
            '',
        ]
        for key, heading in SECTIONS:
            lines += [f'### {heading}', '', report.markdown_table(summary[key]), '']

    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--data', default=str(DEFAULT_DIR),
                        help='directory holding the season CSVs')
    parser.add_argument('--out', default=str(OUT_DIR))
    parser.add_argument('--bands', choices=('walkforward', 'frozen'),
                        default='walkforward')
    parser.add_argument('--warmup-seasons', type=int, default=1,
                        help='seasons replayed but not bet (walkforward only)')
    parser.add_argument('--min-ev', type=float, default=None,
                        help='EV floor as a fraction; defaults to staking.MIN_EV')
    args = parser.parse_args(argv)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    matches = load_all(args.data)
    print(f'Loaded {len(matches):,} matches from {args.data}')

    frozen = None
    if args.bands == 'frozen':
        frozen_path = REPO_ROOT / 'data' / 'elo_bands.json'
        frozen = json.loads(frozen_path.read_text())
        print(f'Using frozen production bands from {frozen_path} '
              f'(LOOKAHEAD — comparison only)')

    kwargs = {'bands_mode': args.bands, 'warmup_seasons': args.warmup_seasons,
              'frozen_bands': frozen}
    if args.min_ev is not None:
        kwargs['min_ev'] = args.min_ev

    result = run(matches, **kwargs)
    print(f"Priced {result['meta']['priced_matches']:,} matches; "
          f"{len(result['fixtures']):,} selection rows; "
          f"{sum(1 for r in result['fixtures'] if r['bet']):,} bets placed")

    summaries = {book: report.build(result, book)
                 for book in result['meta']['books']}

    write_csv(out_dir / 'fixtures.csv', result['fixtures'])
    write_csv(out_dir / 'bets.csv',
              [r for r in result['fixtures'] if r['bet']])
    write_csv(out_dir / 'anomalies.csv', result['anomalies'])
    write_csv(out_dir / 'seeding.csv', result['seeding'])

    flat = []
    for book, summary in summaries.items():
        for key, heading in SECTIONS:
            for row in summary[key]:
                flat.append({'book': book, 'breakdown': key, **row})
    # Calibration rows carry different columns from the metric rows, so the
    # header is the union in first-seen order rather than the first row's keys.
    fieldnames = ['book', 'breakdown']
    for row in flat:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    write_csv(out_dir / 'summary.csv', flat, fieldnames=fieldnames)

    markdown = render_markdown(result, summaries)
    (out_dir / 'summary.md').write_text(markdown, encoding='utf-8')

    print(f'\nWrote fixtures.csv, bets.csv, anomalies.csv, seeding.csv, '
          f'summary.csv, summary.md to {out_dir}/')

    headline = summaries['avg']['overall'][0]
    print(f"\nHeadline (Avg* closing odds, {args.bands} bands): "
          f"{headline['bets']:,} bets, staked {headline['staked']:,.2f}, "
          f"profit {headline['profit']:,.2f}, "
          f"yield {headline['yield_pct']}% , "
          f"flat ROI {headline['flat_roi_pct']}%")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
