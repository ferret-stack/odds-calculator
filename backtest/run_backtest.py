#!/usr/bin/env python3
"""
Run the historical backtest and write the ROI breakdown.

    python3 backtest/run_backtest.py
    python3 backtest/run_backtest.py --bands frozen      # lookahead comparison
    python3 backtest/run_backtest.py --warmup-seasons 2

With no --league this is the ORIGINAL Premier League run on the legacy flat
files (legacy seeding), kept as the regression: 1,298 bets, -10.23% yield,
-11.68% flat ROI on Avg*. It writes to backtest/out/ and nowhere else.

    python3 backtest/run_backtest.py --league E2 E3 --pool
    python3 backtest/run_backtest.py --league E2 E3 --odds avgc --pool

With --league it runs each named league as its own chain (league seeding,
warm-up from the league's earliest season, bets only in --bet-from..--bet-to)
and PRINTS the result. It writes no files.

Reads docs/historical/ and, in --bands frozen, data/elo_bands.json. Nothing
in data/ or pipeline/ is written, and bankroll.json is never opened. The
2025/26 holdout cannot be reached from --league.
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
from backtest.data import (  # noqa: E402
    LEAGUES, ODDS_FAMILIES, entrant_directions, load_league,
    load_legacy_pl_flat, season_label)
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
        title = ('AvgH/AvgD/AvgA — market-average PRE-CLOSING odds (primary)'
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


FAMILY_LABELS = {
    'avg': 'AvgH/AvgD/AvgA, market average, PRE-CLOSING',
    'avgc': 'AvgCH/AvgCD/AvgCA, market average, closing',
    'b365': 'B365H/B365D/B365A, Bet365, pre-closing',
    'psc': 'PSCH/PSCD/PSCA, Pinnacle, closing',
}

E0_CAVEAT = ('E0 here uses league seeding, bets 2019/20 on and a warm-up from '
             '1993/94: NOT comparable to the 1,298-bet legacy PL baseline.')


def _league_row(label, priced, rows):
    summary = report.summarise(rows, label)
    low, high = report.flat_roi_ci(rows)
    return [label, f'{priced:,}', f"{summary['bets']:,}",
            f"{summary['staked']:,.2f}", f"{summary['profit']:,.2f}",
            report._fmt(summary['yield_pct']),
            report._fmt(summary['flat_roi_pct']),
            report._fmt(summary['strike_rate_pct']),
            '-' if low is None else f'{low:.2f} to {high:.2f}']


def league_main(args):
    """Per-league runs, printed only. See the module docstring."""
    family = args.odds
    bet_years = list(range(args.bet_from, args.bet_to + 1))
    bet_labels = [season_label(y) for y in bet_years]

    print(f'Odds: {family} ({FAMILY_LABELS[family]})')
    print(f'Bet seasons: {bet_labels[0]} .. {bet_labels[-1]} | seeding: league '
          f'| bands: walkforward | warm-up: from each league\'s earliest season')
    print('95% CI: flat-stake ROI, normal approximation on per-bet returns')
    print()

    table = [['league', 'matches priced', 'bets', 'staked', 'profit',
              'yield %', 'flat ROI %', 'strike %', '95% CI flat ROI %']]
    pooled_rows, pooled_priced, all_skips, notes = [], 0, [], []
    for code in args.league:
        matches, skips = load_league(code, bet_seasons=bet_years,
                                     families=(family,))
        result = run(matches, seeding='league', league=code,
                     directions=entrant_directions(code),
                     bet_seasons=bet_labels, books=(family,))
        placed = [r for r in result['fixtures'] if r['bet']]
        priced = sum(1 for m in matches
                     if m['season'] in bet_labels and family in m['odds'])
        table.append(_league_row(code, priced, placed))
        pooled_rows += placed
        pooled_priced += priced
        all_skips += skips
        kinds = Counter(a['kind'] for a in result['anomalies'])
        entrants = sum(1 for s in result['seeding']
                       if s['season'] in bet_labels)
        notes.append(f'{code}: {len(matches):,} matches loaded from '
                     f"{result['meta']['seasons'][0]}; entrants seeded in bet "
                     f'seasons: {entrants}; anomalies: '
                     + (', '.join(f'{k} x{v}' for k, v in sorted(kinds.items()))
                        or 'none'))
    if args.pool and len(args.league) > 1:
        table.append(_league_row('POOLED ' + '+'.join(args.league),
                                 pooled_priced, pooled_rows))

    widths = [max(len(row[i]) for row in table) for i in range(len(table[0]))]
    for row in table:
        print('  '.join(cell.ljust(w) for cell, w in zip(row, widths)))

    print('\nRows skipped for a missing price (still fed to ELO), '
          'skipped/rows by league-season:')
    for code in args.league:
        cells = [f"{s['season']} {s['skipped']}/{s['rows']}"
                 for s in all_skips if s['league'] == code]
        total = sum(s['skipped'] for s in all_skips if s['league'] == code)
        print(f'  {code} (total {total}): ' + ', '.join(cells))

    print()
    for note in notes:
        print(note)
    if 'E0' in args.league:
        print(E0_CAVEAT)
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--out', default=str(OUT_DIR))
    parser.add_argument('--bands', choices=('walkforward', 'frozen'),
                        default='walkforward')
    parser.add_argument('--warmup-seasons', type=int, default=1,
                        help='seasons replayed but not bet (walkforward only)')
    parser.add_argument('--min-ev', type=float, default=None,
                        help='EV floor as a fraction; defaults to staking.MIN_EV')
    parser.add_argument('--league', nargs='+', choices=LEAGUES,
                        help='run these leagues (league seeding) and print; '
                             'omit for the legacy flat-file PL regression')
    parser.add_argument('--odds', choices=sorted(ODDS_FAMILIES), default='avg',
                        help='odds family for --league runs')
    parser.add_argument('--bet-from', type=int, default=2020,
                        help='first bet season, by end-year (2020 = 2019/20)')
    parser.add_argument('--bet-to', type=int, default=2025,
                        help='last bet season, by end-year (2025 = 2024/25)')
    parser.add_argument('--pool', action='store_true',
                        help='add a pooled row across the --league codes')
    args = parser.parse_args(argv)

    if args.league:
        if args.bands != 'walkforward' or args.min_ev is not None:
            parser.error('--league runs the unchanged model: walkforward '
                         'bands and the production EV floor only')
        return league_main(args)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    matches, skips = load_legacy_pl_flat()
    print(f'Loaded {len(matches):,} matches (legacy flat PL files); '
          f'rows skipped for a missing price: '
          f'{sum(s["skipped"] for s in skips)}')

    frozen = None
    if args.bands == 'frozen':
        frozen_path = REPO_ROOT / 'data' / 'elo_bands.json'
        frozen = json.loads(frozen_path.read_text())
        print(f'Using frozen production bands from {frozen_path} '
              f'(LOOKAHEAD — comparison only)')

    # The legacy flat PL files reproduce the original result, so they keep
    # the original seeding rule. Every <code>/ league run uses 'league'.
    kwargs = {'bands_mode': args.bands, 'warmup_seasons': args.warmup_seasons,
              'frozen_bands': frozen, 'seeding': 'legacy', 'league': 'E0'}
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
    print(f"\nHeadline (Avg* pre-closing odds, {args.bands} bands): "
          f"{headline['bets']:,} bets, staked {headline['staked']:,.2f}, "
          f"profit {headline['profit']:,.2f}, "
          f"yield {headline['yield_pct']}% , "
          f"flat ROI {headline['flat_roi_pct']}%")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
