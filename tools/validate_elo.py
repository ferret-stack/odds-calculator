"""
Validation harness for the ELO pipeline.

Reports the two measures used for the V6.1 rebuild validation:
  1. Rating-swing distribution -- consecutive per-team ELO movements
     (swings >50pts, >100pts, largest single swing). With K=20 and a MOV
     multiplier capped near 1.7, a well-formed chain cannot move a team by
     more than ~34 points in one match, so any swing above 50 is by
     definition a data defect, not a football result.
  2. Band-level W/D/L split, with Band 1 called out.

Run:  python3 tools/validate_elo.py [--data DIR] [--label NAME]
"""

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path


def load(data_dir):
    with open(Path(data_dir) / 'matches_data.json') as f:
        return json.load(f)


def swing_report(matches):
    """Per-team consecutive ELO movements across the match record."""
    # Ordered by match_id, which is the order the chain is built in. Sorting
    # by date here would manufacture phantom swings wherever the (separately
    # corrupted) dates disagree with true match order.
    by_team = defaultdict(list)
    for m in sorted(matches, key=lambda x: x['match_id']):
        for side in ('home', 'away'):
            elo = m.get(f'{side}_elo')
            if elo is not None:
                by_team[m[f'{side}_team']].append((m['date'], m['match_id'], elo))

    swings = []
    for team, seq in by_team.items():
        for (_, _, prev), (d, mid, cur) in zip(seq, seq[1:]):
            swings.append((abs(cur - prev), team, d, mid))

    swings.sort(reverse=True)
    total = len(swings)
    return {
        'total_transitions': total,
        'over_50': sum(1 for s in swings if s[0] > 50),
        'over_100': sum(1 for s in swings if s[0] > 100),
        'over_200': sum(1 for s in swings if s[0] > 200),
        'largest': swings[0] if swings else None,
        'top5': swings[:5],
        'mean': round(sum(s[0] for s in swings) / total, 2) if total else 0,
    }


def band_report(matches):
    """W/D/L split per band, from the stored winner labels."""
    rows = []
    for band in range(1, 11):
        bm = [m for m in matches if m.get('elo_band') == band]
        if not bm:
            rows.append((band, 0, None, None, None, None))
            continue
        c = Counter(m.get('winner') for m in bm)
        # Evenly-rated matches carry no stronger/weaker information, so they
        # are excluded from the denominator -- the same treatment
        # calculate_elo_bands() applies when building elo_bands.json.
        n = len(bm) - c.get('even', 0)
        if n == 0:
            rows.append((band, 0, None, None, None, None))
            continue
        rows.append((
            band, n,
            c['stronger'] / n, c['draw'] / n, c['weaker'] / n,
            c.get('even', 0) / len(bm),
        ))
    return rows


def integrity_report(matches):
    """Defects that should be zero in a healthy dataset."""
    # Exact ties are legitimate (two teams can hold the same rating); the
    # defect was labelling them as weaker-team wins. So the check is on
    # mislabelled ties, not on ties existing.
    ties = [m for m in matches
            if m.get('home_elo') is not None and m['home_elo'] == m['away_elo']]
    mislabelled_ties = [m for m in ties
                        if m['home_goals'] != m['away_goals']
                        and m.get('winner') != 'even']
    both_default = [m for m in matches if m.get('home_elo') == m.get('away_elo') == 1500]

    mismatched = 0
    for m in matches:
        he, ae, w = m.get('home_elo'), m.get('away_elo'), m.get('winner')
        if he is None or ae is None:
            continue
        hg, ag = m['home_goals'], m['away_goals']
        if hg == ag:
            exp = 'draw'
        elif he == ae:
            exp = 'even'
        elif hg > ag:
            exp = 'stronger' if he > ae else 'weaker'
        else:
            exp = 'stronger' if ae > he else 'weaker'
        if exp != w:
            mismatched += 1

    dates = [m['date'] for m in matches]
    return {
        'ties_mislabelled_as_a_win': len(mislabelled_ties),
        'both_sides_1500': len(both_default),
        'label_contradicts_stored_elo': mismatched,
        'dates_after_season_end': sum(1 for d in dates if d > '2026-06-01'),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--data', default='data')
    ap.add_argument('--label', default=None)
    args = ap.parse_args()

    matches = load(args.data)
    label = args.label or args.data

    print('=' * 68)
    print(f'ELO VALIDATION -- {label}   ({len(matches)} matches)')
    print('=' * 68)

    s = swing_report(matches)
    print('\nRATING-SWING DISTRIBUTION (consecutive per-team ELO movement)')
    print(f"  transitions measured : {s['total_transitions']}")
    print(f"  swings >  50 pts     : {s['over_50']}")
    print(f"  swings > 100 pts     : {s['over_100']}")
    print(f"  swings > 200 pts     : {s['over_200']}")
    print(f"  mean |swing|         : {s['mean']}")
    if s['largest']:
        v, t, d, mid = s['largest']
        print(f"  largest single swing : {v} pts ({t}, {d}, match {mid})")
    print('  top 5:')
    for v, t, d, mid in s['top5']:
        print(f'    {v:5d} pts  {t:<16s} {d}  match {mid}')

    print(f"\n  (exact ties present, correctly excluded: "
          f"{sum(1 for m in matches if m.get('winner') == 'even')})")

    print('\nINTEGRITY CHECKS (all should be 0)')
    for k, v in integrity_report(matches).items():
        flag = '' if v == 0 else '   <-- DEFECT'
        print(f'  {k:<32s}: {v}{flag}')

    print('\nBAND W/D/L SPLIT (from stored winner labels)')
    print(f"  {'band':>4} {'n':>5} {'stronger':>9} {'draw':>7} {'weaker':>7} {'even':>7}")
    for band, n, st, dr, wk, ev in band_report(matches):
        if n == 0:
            print(f'  {band:>4} {n:>5}        --      --      --      --')
            continue
        print(f'  {band:>4} {n:>5} {st:>9.2%} {dr:>7.2%} {wk:>7.2%} {ev:>7.2%}')

    b1 = band_report(matches)[0]
    if b1[1]:
        print(f'\nBAND 1 HEADLINE: stronger {b1[2]:.2%} / draw {b1[3]:.2%} / weaker {b1[4]:.2%}'
              f'  (n={b1[1]})')
        print(f'  stronger-minus-weaker gap: {(b1[2] - b1[4]) * 100:+.2f} pp')


if __name__ == '__main__':
    main()
